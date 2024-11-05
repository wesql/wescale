package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*ExplainCreateTable)(nil)

type ExplainCreateTable struct {
	declarativeDDL *DeclarativeDDL

	noInputs
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

// TryExecute implements Primitive interface
func (e *ExplainCreateTable) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements Primitive interface
func (e *ExplainCreateTable) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return callback(&sqltypes.Result{})
}

// GetFields implements Primitive interface
func (e *ExplainCreateTable) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// todo newborn22
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (e *ExplainCreateTable) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "ExplainCreateTable"
	return rst
}
