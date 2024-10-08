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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Send)(nil)

// Send is an operator to send query to the specific keyspace, tabletType and destination
type Send struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	FieldQuery string

	// IsDML specifies how to deal with autocommit behaviour
	IsDML bool

	// SingleShardOnly specifies that the query must be send to only single shard
	SingleShardOnly bool

	noInputs
}

// NeedsTransaction implements the Primitive interface
func (s *Send) NeedsTransaction() bool {
	return s.IsDML
}

// RouteType implements Primitive interface
func (s *Send) RouteType() string {
	if s.IsDML {
		return "SendDML"
	}

	return "Send"
}

// GetKeyspaceName implements Primitive interface
func (s *Send) GetKeyspaceName() string {
	if s.Keyspace == nil {
		return ""
	}
	return s.Keyspace.Name
}

// GetTableName implements Primitive interface
func (s *Send) GetTableName() string {
	return ""
}

func (s *Send) Resolve(ctx context.Context, vcursor VCursor) ([]*srvtopo.ResolvedShard, error) {
	keyspace := ""
	if s.Keyspace != nil {
		keyspace = s.Keyspace.Name
	}
	rss, err := vcursor.ResolveDefaultDestination(ctx, keyspace, s.TargetDestination)
	return rss, err
}

// TryExecute implements Primitive interface
func (s *Send) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, 0)
	defer cancelFunc()
	var rss []*srvtopo.ResolvedShard
	var err error
	rss, err = s.Resolve(ctx, vcursor)
	if err != nil {
		return nil, err
	}
	if len(rss) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Resolve error, should have at least one shard for query: %v", s.Query)
	}

	if s.Keyspace != nil && !s.Keyspace.Sharded && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		bv := bindVars
		queries[i] = &querypb.BoundQuery{
			Sql:           s.Query,
			BindVariables: bv,
		}
	}

	rollbackOnError := s.IsDML // for non-dml queries, there's no need to do a rollback
	result, errs := vcursor.ExecuteMultiShard(ctx, s, rss, queries, rollbackOnError, s.canAutoCommit(vcursor, rss))
	err = vterrors.Aggregate(errs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Send) canAutoCommit(vcursor VCursor, rss []*srvtopo.ResolvedShard) bool {
	if s.IsDML {
		return len(rss) == 1 && vcursor.AutocommitApproval()
	}
	return false
}

func copyBindVars(in map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// TryStreamExecute implements Primitive interface
func (s *Send) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	var rss []*srvtopo.ResolvedShard
	var err error
	rss, err = s.Resolve(ctx, vcursor)

	if err != nil {
		return err
	}

	if s.Keyspace != nil && !s.Keyspace.Sharded && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range rss {
		bv := bindVars
		multiBindVars[i] = bv
	}
	if s.IsDML {
		result, errors := vcursor.ExecuteMultiShard(ctx, s, rss, []*querypb.BoundQuery{
			{
				Sql:           s.Query,
				BindVariables: bindVars,
			},
		}, true, true)
		if errors != nil {
			return vterrors.Aggregate(errors)
		}
		return callback(result)
	}
	errors := vcursor.StreamExecuteMulti(ctx, s, s.Query, rss, multiBindVars, s.IsDML, s.canAutoCommit(vcursor, rss), callback)
	return vterrors.Aggregate(errors)
}

// GetFields implements Primitive interface
func (s *Send) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	var rss []*srvtopo.ResolvedShard
	var err error
	rss, err = s.Resolve(ctx, vcursor)
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, fmt.Errorf("no shards for keyspace: [%s]", s.GetKeyspaceName())
	}
	qr, err := execShard(ctx, s, vcursor, s.FieldQuery, bindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (s *Send) description() PrimitiveDescription {
	other := map[string]any{
		"Query": s.Query,
		"Table": s.GetTableName(),
	}
	if s.IsDML {
		other["IsDML"] = true
	}
	if s.SingleShardOnly {
		other["SingleShardOnly"] = true
	}
	return PrimitiveDescription{
		OperatorType:      "Send",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,
		Other:             other,
	}
}
