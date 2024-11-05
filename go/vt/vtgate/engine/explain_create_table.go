package engine

import (
	"context"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*ExplainCreateTable)(nil)

type ExplainCreateTable struct {
	declarativeDDL *DeclarativeDDL

	noInputs
}

func BuildExplainCreateTablePlan(createTableStatement *sqlparser.CreateTable) *ExplainCreateTable {
	explainCreateTable := &ExplainCreateTable{}
	explainCreateTable.declarativeDDL = BuildDeclarativeDDLPlan(createTableStatement)
	return explainCreateTable
}

// NeedsTransaction implements the Primitive interface
func (e *ExplainCreateTable) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (e *ExplainCreateTable) RouteType() string {
	return "ExplainCreateTable"
}

// GetKeyspaceName implements Primitive interface
func (e *ExplainCreateTable) GetKeyspaceName() string {
	return e.declarativeDDL.dbName
}

// GetTableName implements Primitive interface
func (e *ExplainCreateTable) GetTableName() string {
	return e.declarativeDDL.tableName
}

var colName = "DDLs to Execute"

// TryExecute implements Primitive interface
func (e *ExplainCreateTable) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := e.declarativeDDL.calculateDiff(ctx, vcursor)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	row := make([][]sqltypes.Value, len(e.declarativeDDL.diffDDLs))
	for i, diff := range e.declarativeDDL.diffDDLs {
		row[i] = sqltypes.BuildVarCharRow(diff)
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(colName),
		Rows:   row,
	}, nil
}

// TryStreamExecute implements Primitive interface
func (e *ExplainCreateTable) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := e.declarativeDDL.calculateDiff(ctx, vcursor)
	if err != nil {
		return err
	}

	row := make([][]sqltypes.Value, len(e.declarativeDDL.diffDDLs))
	for i, diff := range e.declarativeDDL.diffDDLs {
		row[i] = sqltypes.BuildVarCharRow(diff)
	}

	return callback(&sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(colName),
		Rows:   row,
	})
}

// GetFields implements Primitive interface
func (e *ExplainCreateTable) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{Fields: sqltypes.BuildVarCharFields(colName)}, nil
}

// description implements the Primitive interface
func (e *ExplainCreateTable) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "ExplainCreateTable"
	return rst
}
