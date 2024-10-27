/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package engine

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*DeclarativeDDL)(nil)

// DeclarativeDDL is an operator to send schema diff DDL queries to the specific keyspace, tabletType and destination
type DeclarativeDDL struct {
	// set when plan building
	dbName        string // will set to session db when executing if sql db is not provided
	tableName     string
	desiredSchema string

	// set when execute
	originSchema string
	diffDDL      string
	diffDDLStmt  sqlparser.DDLStatement

	isDirect bool
	// query will be set when execute
	directPrimitive    *Send
	OnlineDDLPrimitive *OnlineDDL

	noInputs
}

func BuildDeclarativeDDLPlan(createTableStatement *sqlparser.CreateTable, send *Send, onlineDDL *OnlineDDL) *DeclarativeDDL {
	// plan will be cached and index by sql template, so here we just build some static info related the sql.

	sqlDBName := createTableStatement.GetTable().Qualifier.String()
	tableName := createTableStatement.GetTable().Name.String()
	desireSchema := sqlparser.CanonicalString(createTableStatement)

	directPrimitive := &Send{}
	directPrimitive.TargetDestination = send.TargetDestination
	//directPrimitive.Keyspace = &vindexes.Keyspace{Sharded: send.Keyspace.Sharded}
	directPrimitive.Keyspace = send.Keyspace

	OnlineDDLPrimitive := &OnlineDDL{}
	OnlineDDLPrimitive.TargetDestination = onlineDDL.TargetDestination
	//OnlineDDLPrimitive.Keyspace = &vindexes.Keyspace{Sharded: onlineDDL.Keyspace.Sharded}
	OnlineDDLPrimitive.Keyspace = onlineDDL.Keyspace

	return &DeclarativeDDL{
		dbName:             sqlDBName,
		tableName:          tableName,
		desiredSchema:      desireSchema,
		directPrimitive:    directPrimitive,
		OnlineDDLPrimitive: OnlineDDLPrimitive,
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
	hints := &schemadiff.DiffHints{
		// todo clint: test different TableCharsetCollateStrategy
		TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways,
		// todo clint: add to pflag
		AlterTableAlgorithmStrategy: schemadiff.AlterTableAlgorithmStrategyNone,
	}
	diff, err := schemadiff.DiffCreateTablesQueries(d.originSchema, d.desiredSchema, hints)
	if err != nil {
		return err
	}

	ddlStmt, ok := diff.Statement().(sqlparser.DDLStatement)
	if !ok {
		return fmt.Errorf("diff ddl is not a DDLStatement")
	}
	d.diffDDLStmt = ddlStmt
	// if we don't set dbName here, it will be set to mysql db when executing
	ddlStmt.SetTable(d.dbName, d.tableName)
	ddlStmt.SetFullyParsed(true)
	d.diffDDL = diff.CanonicalStatementString()

	return nil
}

func (d *DeclarativeDDL) initSubPrimitive(cursor VCursor) error {
	if schema.DDLStrategy(cursor.Session().GetDDLStrategy()) == schema.DDLStrategyDirect {
		d.isDirect = true
		d.directPrimitive.Query = d.diffDDL
		return nil
	}
	d.OnlineDDLPrimitive.SQL = d.diffDDL
	d.OnlineDDLPrimitive.DDL = d.diffDDLStmt

	ddlStrategySetting, err := schema.ParseDDLStrategy(cursor.Session().GetDDLStrategy())
	if err != nil {
		return err
	}
	d.OnlineDDLPrimitive.DDLStrategySetting = ddlStrategySetting

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
		return vcursor.ExecutePrimitive(ctx, d.directPrimitive, bindVars, wantfields)
	}
	return vcursor.ExecutePrimitive(ctx, d.OnlineDDLPrimitive, bindVars, wantfields)
}

// TryStreamExecute implements Primitive interface
func (d *DeclarativeDDL) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := d.Init(ctx, vcursor)
	if err != nil {
		return err
	}
	if d.isDirect {
		return vcursor.StreamExecutePrimitive(ctx, d.directPrimitive, bindVars, wantfields, callback)
	}
	return vcursor.StreamExecutePrimitive(ctx, d.OnlineDDLPrimitive, bindVars, wantfields, callback)
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
