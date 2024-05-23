/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

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

	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/vt/key"
	querypb "github.com/wesql/wescale/go/vt/proto/query"
	"github.com/wesql/wescale/go/vt/vterrors"
	"github.com/wesql/wescale/go/vt/vtgate/vindexes"
)

var _ Primitive = (*MStream)(nil)

// MStream is an operator for message streaming from specific keyspace, destination
type MStream struct {
	// Keyspace specifies the keyspace to stream messages from
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to stream messages from
	TargetDestination key.Destination

	// TableName specifies the table on which stream will be executed.
	TableName string

	noTxNeeded

	noInputs
}

// RouteType implements the Primitive interface
func (m *MStream) RouteType() string {
	return "MStream"
}

// GetKeyspaceName implements the Primitive interface
func (m *MStream) GetKeyspaceName() string {
	if m.Keyspace == nil {
		return ""
	}
	return m.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (m *MStream) GetTableName() string {
	return m.TableName
}

// TryExecute implements the Primitive interface
func (m *MStream) TryExecute(_ context.Context, _ VCursor, _ map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("TryExecute is not supported for MStream")
}

// TryStreamExecute implements the Primitive interface
func (m *MStream) TryStreamExecute(ctx context.Context, vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	rss, _, err := vcursor.ResolveDestinations(ctx, m.Keyspace.Name, nil, []key.Destination{m.TargetDestination})
	if err != nil {
		return err
	}
	return vcursor.MessageStream(ctx, rss, m.TableName, callback)
}

// GetFields implements the Primitive interface
func (m *MStream) GetFields(_ context.Context, _ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("GetFields is not supported for MStream")
}

func (m *MStream) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "MStream",
		Keyspace:          m.Keyspace,
		TargetDestination: m.TargetDestination,

		Other: map[string]any{"Table": m.TableName},
	}
}
