/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package engine

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*DeclarativeDDL)(nil)

type HintsInStr struct {
	StrictIndexOrdering         bool
	AutoIncrementStrategy       string
	RangeRotationStrategy       string
	ConstraintNamesStrategy     string
	ColumnRenameStrategy        string
	TableRenameStrategy         string
	FullTextKeyStrategy         string
	TableCharsetCollateStrategy string
	AlterTableAlgorithmStrategy string
}

var defaultConfig = HintsInStr{
	StrictIndexOrdering:         false,
	AutoIncrementStrategy:       schemadiff.AutoIncrementStrategyValues[schemadiff.AutoIncrementIgnore],
	RangeRotationStrategy:       schemadiff.RangeRotationStrategyValues[schemadiff.RangeRotationFullSpec],
	ConstraintNamesStrategy:     schemadiff.ConstraintNamesStrategyValues[schemadiff.ConstraintNamesIgnoreVitess],
	ColumnRenameStrategy:        schemadiff.ColumnRenameStrategyValues[schemadiff.ColumnRenameHeuristicStatement],
	TableRenameStrategy:         schemadiff.TableRenameStrategyValues[schemadiff.TableRenameHeuristicStatement], // in our case, only one table
	FullTextKeyStrategy:         schemadiff.FullTextKeyStrategyValues[schemadiff.FullTextKeyDistinctStatements],
	TableCharsetCollateStrategy: schemadiff.TableCharsetCollateStrategyValues[schemadiff.TableCharsetCollateIgnoreAlways],
	AlterTableAlgorithmStrategy: schemadiff.AlterTableAlgorithmStrategyValues[schemadiff.AlterTableAlgorithmStrategyNone],
}

var configInStr = HintsInStr{
	StrictIndexOrdering:         defaultConfig.StrictIndexOrdering,
	AutoIncrementStrategy:       defaultConfig.AutoIncrementStrategy,
	RangeRotationStrategy:       defaultConfig.RangeRotationStrategy,
	ConstraintNamesStrategy:     defaultConfig.ConstraintNamesStrategy,
	ColumnRenameStrategy:        defaultConfig.ColumnRenameStrategy,
	TableRenameStrategy:         defaultConfig.TableRenameStrategy,
	FullTextKeyStrategy:         defaultConfig.FullTextKeyStrategy,
	TableCharsetCollateStrategy: defaultConfig.TableCharsetCollateStrategy,
	AlterTableAlgorithmStrategy: defaultConfig.AlterTableAlgorithmStrategy,
}

// not all options are supported to configure
func registerPoolSizeControllerConfigTypeFlags(fs *pflag.FlagSet) {
	fs.StringVar(&configInStr.AutoIncrementStrategy, "declarative_ddl_hints_auto_increment_strategy", configInStr.AutoIncrementStrategy, "auto increment strategy")
	fs.StringVar(&configInStr.RangeRotationStrategy, "declarative_ddl_hints_range_rotation_strategy", configInStr.RangeRotationStrategy, "range rotation strategy")
	fs.StringVar(&configInStr.ConstraintNamesStrategy, "declarative_ddl_hints_constraint_names_strategy", configInStr.ConstraintNamesStrategy, "constraint names strategy")
	fs.StringVar(&configInStr.ColumnRenameStrategy, "declarative_ddl_hints_column_rename_strategy", configInStr.ColumnRenameStrategy, "column rename strategy")
	fs.StringVar(&configInStr.AlterTableAlgorithmStrategy, "declarative_ddl_hints_alter_table_algorithm_strategy", configInStr.AlterTableAlgorithmStrategy, "set table algorithm strategy")
	// todo newborn22: fix before enabling configure
	// fs.StringVar(&configInStr.TableCharsetCollateStrategy, "declarative_ddl_hints_table_charset_collate_strategy", configInStr.TableCharsetCollateStrategy, "table charset collate strategy")
}

func init() {
	servenv.OnParseFor("vtgate", registerPoolSizeControllerConfigTypeFlags)
}

// DeclarativeDDL is an operator to send schema diff DDL queries to the specific keyspace, tabletType and destination
type DeclarativeDDL struct {
	// set when plan building
	dbName        string // will set to session db when executing if sql db is not provided
	tableName     string
	desiredSchema string

	// set when execute
	originSchema string
	diffDDLs     []string
	diffDDLStmts []sqlparser.DDLStatement

	isDirect            bool
	directPrimitives    []*Send
	onlineDDLPrimitives []*OnlineDDL

	noInputs
}

func BuildDeclarativeDDLPlan(createTableStatement *sqlparser.CreateTable) *DeclarativeDDL {
	// plan will be cached and index by sql template, so here we just build some static info related the sql.

	sqlDBName := createTableStatement.GetTable().Qualifier.String()
	tableName := createTableStatement.GetTable().Name.String()
	normalizeCreateTableStmt(createTableStatement)
	desireSchema := sqlparser.CanonicalString(createTableStatement)

	return &DeclarativeDDL{
		dbName:        sqlDBName,
		tableName:     tableName,
		desiredSchema: desireSchema,
	}
}

func normalizeCreateTableStmt(createTableStatement *sqlparser.CreateTable) {
	// pk column should be not null too
	for _, col := range createTableStatement.TableSpec.Columns {
		if col.Type.Options.KeyOpt == sqlparser.ColKeyPrimary {
			v := false
			col.Type.Options.Null = &v
		}
	}
}

func (d *DeclarativeDDL) calculateDiff(ctx context.Context, cursor VCursor) error {
	th, err := cursor.FindHealthyPrimaryTablet()
	if err != nil {
		return err
	}

	sessionDB := cursor.GetKeyspace()
	if d.dbName == "" {
		d.dbName = sessionDB
		if d.dbName == "" {
			return fmt.Errorf("no database selected")
		}
	}

	// return error if database not exist
	qr, err := th.Conn.ExecuteInternal(ctx, th.Target,
		fmt.Sprintf("SELECT SCHEMA_NAME\nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '%v';", d.dbName),
		nil, 0, 0, nil)
	if err != nil {
		return err
	}
	dbExist := len(qr.Rows) > 0
	if !dbExist {
		return fmt.Errorf("database %v not exist", d.dbName)
	}

	// get origin schema
	qr, err = th.Conn.ExecuteInternal(ctx, th.Target,
		fmt.Sprintf("SHOW CREATE TABLE %v.%v;", d.dbName, d.tableName),
		nil, 0, 0, nil)
	if err != nil {
		if vterrors.Code(err) == vtrpcpb.Code_NOT_FOUND {
			// table not exist yet
			d.originSchema = ""
		} else {
			return err
		}
	} else {
		if len(qr.Rows) == 1 {
			d.originSchema = qr.Rows[0][1].ToString()
		} else {
			return fmt.Errorf("the len of result from show create table %v.%v is not 1 but %v", d.dbName, d.tableName, len(qr.Rows))
		}
	}

	// get diff DDL
	d.diffDDLs = make([]string, 0)
	d.diffDDLStmts = make([]sqlparser.DDLStatement, 0)

	hints := ConvertHints(configInStr)
	diff, err := schemadiff.DiffCreateTablesQueries(d.originSchema, d.desiredSchema, &hints)
	if err != nil {
		return err
	}
	if diff.IsEmpty() {
		return nil
	}

	for diff != nil && !diff.IsEmpty() {
		ddlStmt, ok := diff.Statement().(sqlparser.DDLStatement)
		if !ok {
			return fmt.Errorf("diff ddl is not a DDLStatement")
		}

		// if we don't set dbName here, it will be set to mysql db when executing
		ddlStmt.SetTable(d.dbName, d.tableName)
		ddlStmt.SetFullyParsed(true)
		d.diffDDLStmts = append(d.diffDDLStmts, ddlStmt)
		d.diffDDLs = append(d.diffDDLs, diff.CanonicalStatementString())

		diff = diff.SubsequentDiff()
	}

	return nil
}

func (d *DeclarativeDDL) initSubPrimitive(cursor VCursor) error {
	d.directPrimitives = make([]*Send, 0)
	d.onlineDDLPrimitives = make([]*OnlineDDL, 0)

	if schema.DDLStrategy(cursor.Session().GetDDLStrategy()) == schema.DDLStrategyDirect {
		d.isDirect = true
		for _, diffDDL := range d.diffDDLs {
			send := &Send{Query: diffDDL,
				Keyspace:          &vindexes.Keyspace{Name: d.dbName, Sharded: false},
				TargetDestination: key.DestinationShard("0")}
			d.directPrimitives = append(d.directPrimitives, send)
		}
		return nil
	}

	ddlStrategySetting, err := schema.ParseDDLStrategy(cursor.Session().GetDDLStrategy())
	if err != nil {
		return err
	}
	for i, diffDDL := range d.diffDDLs {
		onlineDDL := &OnlineDDL{SQL: diffDDL, DDL: d.diffDDLStmts[i], DDLStrategySetting: ddlStrategySetting,
			Keyspace:          &vindexes.Keyspace{Name: d.dbName, Sharded: false},
			TargetDestination: key.DestinationShard("0")}
		d.onlineDDLPrimitives = append(d.onlineDDLPrimitives, onlineDDL)
	}
	return nil
}

// Init should be called before execution
func (d *DeclarativeDDL) Init(ctx context.Context, cursor VCursor) error {
	err := d.calculateDiff(ctx, cursor)
	if err != nil {
		return err
	}
	return d.initSubPrimitive(cursor)
}

// NeedsTransaction implements the Primitive interface
func (d *DeclarativeDDL) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (d *DeclarativeDDL) RouteType() string {
	return "DeclarativeDDL"
}

// GetKeyspaceName implements Primitive interface
func (d *DeclarativeDDL) GetKeyspaceName() string {
	return d.dbName
}

// GetTableName implements Primitive interface
func (d *DeclarativeDDL) GetTableName() string {
	return d.tableName
}

// TryExecute implements Primitive interface
func (d *DeclarativeDDL) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := d.Init(ctx, vcursor)
	if err != nil {
		return nil, err
	}
	if d.isDirect {
		for _, send := range d.directPrimitives {
			_, err = vcursor.ExecutePrimitive(ctx, send, bindVars, wantfields)
			if err != nil {
				return nil, err
			}
		}
		return &sqltypes.Result{}, nil
	}

	rows := make([][]sqltypes.Value, 0)
	for _, onlineDDL := range d.onlineDDLPrimitives {
		qr, err := vcursor.ExecutePrimitive(ctx, onlineDDL, bindVars, wantfields)
		if err != nil {
			return nil, err
		}
		if len(qr.Named().Rows) != 1 {
			return nil, fmt.Errorf("DeclarativeDDL: the len of result from online ddl is not 1 but %v", len(qr.Named().Rows))
		}
		uuid := qr.Named().Rows[0].AsString("uuid", "")
		rows = append(rows, sqltypes.BuildVarCharRow(uuid))
	}

	return &sqltypes.Result{Fields: sqltypes.BuildVarCharFields("uuid"), Rows: rows}, nil
}

// TryStreamExecute implements Primitive interface
func (d *DeclarativeDDL) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := d.Init(ctx, vcursor)
	if err != nil {
		return err
	}
	if d.isDirect {
		for _, send := range d.directPrimitives {
			err = vcursor.StreamExecutePrimitive(ctx, send, bindVars, wantfields, func(result *sqltypes.Result) error {
				// we don't care about the result
				return nil
			})
			if err != nil {
				return err
			}
		}
		return callback(&sqltypes.Result{})
	}

	rows := make([][]sqltypes.Value, 0)
	for _, onlineDDL := range d.onlineDDLPrimitives {
		err := vcursor.StreamExecutePrimitive(ctx, onlineDDL, bindVars, wantfields, func(qr *sqltypes.Result) error {
			if len(qr.Named().Rows) != 1 {
				return fmt.Errorf("DeclarativeDDL: the len of result from online ddl is not 1 but %v", len(qr.Named().Rows))
			}
			uuid := qr.Named().Rows[0].AsString("uuid", "")
			rows = append(rows, sqltypes.BuildVarCharRow(uuid))
			return nil
		})
		if err != nil {
			return err
		}
	}

	return callback(&sqltypes.Result{Fields: sqltypes.BuildVarCharFields("uuid"), Rows: rows})
}

// GetFields implements Primitive interface
func (d *DeclarativeDDL) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (d *DeclarativeDDL) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "DeclarativeDDL"
	return rst
}

func ConvertHints(hints HintsInStr) schemadiff.DiffHints {
	var diffHints schemadiff.DiffHints

	diffHints.StrictIndexOrdering = hints.StrictIndexOrdering

	// AutoIncrementStrategy
	if strategy, err := schemadiff.ParseAutoIncrementStrategy(hints.AutoIncrementStrategy); err == nil {
		diffHints.AutoIncrementStrategy = strategy
	} else {
		diffHints.AutoIncrementStrategy, _ = schemadiff.ParseAutoIncrementStrategy(defaultConfig.AutoIncrementStrategy)
	}

	// RangeRotationStrategy
	if strategy, err := schemadiff.ParseRangeRotationStrategy(hints.RangeRotationStrategy); err == nil {
		diffHints.RangeRotationStrategy = strategy
	} else {
		diffHints.RangeRotationStrategy, _ = schemadiff.ParseRangeRotationStrategy(defaultConfig.RangeRotationStrategy)
	}

	// ConstraintNamesStrategy
	if strategy, err := schemadiff.ParseConstraintNamesStrategy(hints.ConstraintNamesStrategy); err == nil {
		diffHints.ConstraintNamesStrategy = strategy
	} else {
		diffHints.ConstraintNamesStrategy, _ = schemadiff.ParseConstraintNamesStrategy(defaultConfig.ConstraintNamesStrategy)
	}

	// ColumnRenameStrategy
	if strategy, err := schemadiff.ParseColumnRenameStrategy(hints.ColumnRenameStrategy); err == nil {
		diffHints.ColumnRenameStrategy = strategy
	} else {
		diffHints.ColumnRenameStrategy, _ = schemadiff.ParseColumnRenameStrategy(defaultConfig.ColumnRenameStrategy)
	}

	// TableRenameStrategy
	if strategy, err := schemadiff.ParseTableRenameStrategy(hints.TableRenameStrategy); err == nil {
		diffHints.TableRenameStrategy = strategy
	} else {
		diffHints.TableRenameStrategy, _ = schemadiff.ParseTableRenameStrategy(defaultConfig.TableRenameStrategy)
	}

	// FullTextKeyStrategy
	if strategy, err := schemadiff.ParseFullTextKeyStrategy(hints.FullTextKeyStrategy); err == nil {
		diffHints.FullTextKeyStrategy = strategy
	} else {
		diffHints.FullTextKeyStrategy, _ = schemadiff.ParseFullTextKeyStrategy(defaultConfig.FullTextKeyStrategy)
	}

	// TableCharsetCollateStrategy
	if strategy, err := schemadiff.ParseTableCharsetCollateStrategy(hints.TableCharsetCollateStrategy); err == nil {
		diffHints.TableCharsetCollateStrategy = strategy
	} else {
		diffHints.TableCharsetCollateStrategy, _ = schemadiff.ParseTableCharsetCollateStrategy(defaultConfig.TableCharsetCollateStrategy)
	}

	// AlterTableAlgorithmStrategy
	if strategy, err := schemadiff.ParseAlterTableAlgorithmStrategy(hints.AlterTableAlgorithmStrategy); err == nil {
		diffHints.AlterTableAlgorithmStrategy = strategy
	} else {
		diffHints.AlterTableAlgorithmStrategy, _ = schemadiff.ParseAlterTableAlgorithmStrategy(defaultConfig.AlterTableAlgorithmStrategy)
	}

	return diffHints
}
