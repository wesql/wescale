/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"context"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*UpdateTarget)(nil)

// UpdateTarget is an operator to update target string.
type UpdateTarget struct {
	// Target string to be updated
	Target string

	noInputs

	noTxNeeded
}

func (updTarget *UpdateTarget) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "UpdateTarget",
		Other:        map[string]any{"target": updTarget.Target},
	}
}

// RouteType implements the Primitive interface
func (updTarget *UpdateTarget) RouteType() string {
	return "UpdateTarget"
}

// GetKeyspaceName implements the Primitive interface
func (updTarget *UpdateTarget) GetKeyspaceName() string {
	return updTarget.Target
}

// GetTableName implements the Primitive interface
func (updTarget *UpdateTarget) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (updTarget *UpdateTarget) TryExecute(_ context.Context, vcursor VCursor, _ map[string]*query.BindVariable, _ bool) (*sqltypes.Result, error) {
	err := vcursor.Session().SetTarget(updTarget.Target, true)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface
func (updTarget *UpdateTarget) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result, err := updTarget.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(result)
}

// GetFields implements the Primitive interface
func (updTarget *UpdateTarget) GetFields(_ context.Context, _ VCursor, _ map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields not reachable for use statement")
}
