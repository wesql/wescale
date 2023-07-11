/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func buildPlanForBypass(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	return buildPlanForBypassWithLocks(stmt, reservedVars, vschema, nil)
}

func buildPlanForBypassWithLocks(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, vschema plancontext.VSchema, lockList []*engine.SessionLock) (*planResult, error) {
	keyspace, err := vschema.DefaultKeyspace()
	// If no keyspace is specified in this SQL or Session, the SQL can be processed directly by vttablet,
	// because vttablet can now handle SQL without any database specified.
	if err != nil && err.Error() != vterrors.VT09005().Error() {
		return nil, err
	}

	switch dest := vschema.Destination().(type) {
	case key.DestinationExactKeyRange:
		if _, ok := stmt.(*sqlparser.Insert); ok {
			return nil, vterrors.VT03023(vschema.TargetString())
		}
	case key.DestinationShard:
		if !vschema.IsShardRoutingEnabled() {
			break
		}
		shard := string(dest)
		targetKeyspace, err := GetShardRoute(vschema, keyspace.Name, shard)
		if err != nil {
			return nil, err
		}
		if targetKeyspace != nil {
			keyspace = targetKeyspace
		}
	}

	isDML := sqlparser.IsDMLStatement(stmt)
	fieldQuery := ""
	if !isDML {
		buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
		node := buffer.WriteNode(stmt)
		fieldQuery = node.ParsedQuery().Query
	}
	send := &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: vschema.Destination(),
		Query:             sqlparser.String(stmt),
		FieldQuery:        fieldQuery,
		IsDML:             isDML,
		SingleShardOnly:   false,
	}

	if lockList != nil && len(lockList) != 0 {
		send.LockFuncs = lockList
	}

	sel, isSel := stmt.(*sqlparser.Select)
	if isSel && isOnlyDual(sel) {
		used := "dual"
		if keyspace != nil && keyspace.Name != "" {
			// we are just getting the ks to log the correct table use.
			used = keyspace.Name + ".dual"
		}
		return newPlanResult(send, used), nil
	}
	return newPlanResult(send), nil
}

func GetShardRoute(vschema plancontext.VSchema, keyspace, shard string) (*vindexes.Keyspace, error) {
	targetKeyspaceName, err := vschema.FindRoutedShard(keyspace, shard)
	if err != nil {
		return nil, err
	}
	return vschema.FindKeyspace(targetKeyspaceName)
}
