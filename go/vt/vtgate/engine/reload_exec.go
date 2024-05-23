/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package engine

import (
	"context"

	"github.com/wesql/wescale/go/sqltypes"
	querypb "github.com/wesql/wescale/go/vt/proto/query"
	"github.com/wesql/wescale/go/vt/sqlparser"
)

var _ Primitive = (*ReloadExec)(nil)

type ReloadExec struct {
	ReloadType sqlparser.ReloadType

	noInputs
	noTxNeeded
}

func (r *ReloadExec) RouteType() string {
	return "ReloadExec"
}

func (r *ReloadExec) GetKeyspaceName() string {
	return ""
}

func (r *ReloadExec) GetTableName() string {
	return ""
}

func (r ReloadExec) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := r.TryExecute(ctx, vcursor, bindVars, true)
	if err != nil {
		return nil, err
	}
	qr.Rows = nil
	return qr, nil
}

func (r *ReloadExec) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return vcursor.ReloadExec(ctx, r.ReloadType)
}

func (r *ReloadExec) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := r.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)

}

func (r *ReloadExec) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ReloadExec",
		Variant:      r.ReloadType.ToString(),
	}
}
