package engine

import (
	"context"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*Branch)(nil)

// Branch is an operator to deal with branch commands
type Branch struct {
	// set when plan building
	name        string
	commandType string
	params      map[string]string

	noInputs
}

func BuildBranchPlan(branchCmd *sqlparser.BranchCommand) *Branch {
	// todo complete me
	// plan will be cached and index by sql template, so here we just build some static info related the sql.
	return &Branch{}
}

// todo complete me
// TryExecute implements Primitive interface
func (b *Branch) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	return result, nil
}

// todo complete me
// TryStreamExecute implements Primitive interface
func (b *Branch) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result := &sqltypes.Result{}
	return callback(result)
}

// GetFields implements Primitive interface
func (b *Branch) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (b *Branch) description() PrimitiveDescription {
	var rst PrimitiveDescription
	rst.OperatorType = "Branch"
	return rst
}

// NeedsTransaction implements the Primitive interface
func (b *Branch) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (b *Branch) RouteType() string {
	return "Branch"
}

// GetKeyspaceName implements Primitive interface
func (b *Branch) GetKeyspaceName() string {
	return ""
}

// GetTableName implements Primitive interface
func (b *Branch) GetTableName() string {
	return ""
}
