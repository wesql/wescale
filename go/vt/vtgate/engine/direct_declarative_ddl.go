/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package engine

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*DirectDeclarativeDDL)(nil)

// DirectDeclarativeDDL is an operator to send schema diff DDL queries to the specific keyspace, tabletType and destination
type DirectDeclarativeDDL struct {
	dbName        string // todo clint: create db if not exist
	dbExist       bool
	tableName     string
	originSchema  string
	desiredSchema string
	diffDDL       string
}

func InitDirectDeclarativeDDL(ctx context.Context, ddlStatement *sqlparser.CreateTable, cursor VCursor) (*DirectDeclarativeDDL, error) {
	th, err := cursor.FindHealthyPrimaryTablet()
	if err != nil {
		return nil, err
	}

	var dbName string
	var tableName string
	var dbExist bool
	var originSchema string
	var desireSchema string
	var diffDDL string

	dbName = ddlStatement.GetTable().Qualifier.String()
	tableName = ddlStatement.GetTable().Name.String()

	qr, err := th.Conn.ExecuteInternal(ctx, th.Target, fmt.Sprintf("SELECT SCHEMA_NAME\nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '%v';", dbName),
		nil, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	dbExist = len(qr.Rows) > 0
	if dbExist {
		qr, err := th.Conn.ExecuteInternal(ctx, th.Target, fmt.Sprintf("SHOW CREATE TABLE %v.%v", dbName, tableName),
			nil, 0, 0, nil)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) == 0 {
			originSchema = ""
		} else {
			originSchema = qr.Rows[0][1].ToString()
		}
	}

	desireSchema = sqlparser.CanonicalString(ddlStatement)

	hints := &schemadiff.DiffHints{
		// todo review and test: copy, inplace, instant
		TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways,
		// todo review: copy, inplace, instant
		AlterTableAlgorithmStrategy: schemadiff.AlterTableAlgorithmStrategyNone,
	}
	diff, err := schemadiff.DiffCreateTablesQueries(originSchema, desireSchema, hints)
	if err != nil {
		return nil, err
	}

	diffDDL = diff.CanonicalStatementString()
	log.Debugf("the diff DDLs to execute is %v", diffDDL)

	return &DirectDeclarativeDDL{
		dbName:        dbName,
		dbExist:       dbExist,
		tableName:     tableName,
		originSchema:  originSchema,
		desiredSchema: desireSchema,
		diffDDL:       diffDDL,
	}, nil
}

// NeedsTransaction implements the Primitive interface
func (d *DirectDeclarativeDDL) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (d *DirectDeclarativeDDL) RouteType() string {
	return "DirectDeclarativeDDL"
}

// GetKeyspaceName implements Primitive interface
func (d *DirectDeclarativeDDL) GetKeyspaceName() string {
	return d.dbName
}

// GetTableName implements Primitive interface
func (d *DirectDeclarativeDDL) GetTableName() string {
	return d.tableName
}

// TryExecute implements Primitive interface
func (d *DirectDeclarativeDDL) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	th, err := vcursor.FindHealthyPrimaryTablet()
	if err != nil {
		return nil, err
	}

	if !d.dbExist {
		_, err = th.Conn.ExecuteInternal(ctx, th.Target, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v", d.dbName), nil, 0, 0, nil)
		if err != nil {
			return nil, err
		}
		return th.Conn.ExecuteInternal(ctx, th.Target, d.desiredSchema, nil, 0, 0, nil)
	}

	_, err = th.Conn.ExecuteInternal(ctx, th.Target, d.diffDDL, nil, 0, 0, nil)
	if err != nil {
		return nil, err
	}

	// build final result
	// todo review: keep or remove?
	fileNames := []string{"Diff DDL", "Result"}
	var resultRows []sqltypes.Row
	row := sqltypes.BuildVarCharRow(d.diffDDL, "succeed")
	resultRows = append(resultRows, row)

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(fileNames...),
		Rows:   resultRows,
	}, nil
}

// TryStreamExecute implements Primitive interface
func (d *DirectDeclarativeDDL) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	th, err := vcursor.FindHealthyPrimaryTablet()
	if err != nil {
		return err
	}

	if !d.dbExist {
		_, err = th.Conn.ExecuteInternal(ctx, th.Target, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v", d.dbName), nil, 0, 0, nil)
		if err != nil {
			return err
		}
		qr, err := th.Conn.ExecuteInternal(ctx, th.Target, d.desiredSchema, nil, 0, 0, nil)
		if err != nil {
			return err
		}
		return callback(qr)
	}

	_, err = th.Conn.ExecuteInternal(ctx, th.Target, d.diffDDL, nil, 0, 0, nil)
	if err != nil {
		return err
	}

	// build final result
	// todo review: keep or remove?
	fileNames := []string{"Diff DDL", "Result"}
	var resultRows []sqltypes.Row
	row := sqltypes.BuildVarCharRow(d.diffDDL, "succeed")
	resultRows = append(resultRows, row)

	qr := &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(fileNames...),
		Rows:   resultRows,
	}

	return callback(qr)
}

// GetFields implements Primitive interface
func (d *DirectDeclarativeDDL) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Diff DDL", "Result"),
	}, nil
}

// Inputs implements the Primitive interface
func (d *DirectDeclarativeDDL) Inputs() []Primitive {
	return []Primitive{}
}

// description implements the Primitive interface
func (d *DirectDeclarativeDDL) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "DirectDeclarativeDDL"
	return rst
}
