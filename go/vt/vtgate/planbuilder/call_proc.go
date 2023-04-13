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

package planbuilder

import (
	"vitess.io/vitess/go/internal/global"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func buildCallProcPlan(stmt *sqlparser.CallProc, vschema plancontext.VSchema) (*planResult, error) {
	var ks string
	var keyspace *vindexes.Keyspace
	var dest key.Destination
	if !stmt.Name.Qualifier.IsEmpty() {
		ks = stmt.Name.Qualifier.String()
	}

	// wesql-server support system package dbms_consensus.
	// if call dbms_consensus.show_logs(), we should send to default keyspace.
	isSystemSchema := sqlparser.SystemSchema(ks)
	if isSystemSchema {
		defaultKeyspace, err := vschema.FindKeyspace(global.DefaultKeyspace)
		if err != nil {
			return nil, err
		}
		keyspace = defaultKeyspace
		dest = nil
	} else {
		targetDest, targetKeyspace, _, err := vschema.TargetDestination(ks)
		if err != nil {
			return nil, err
		}
		keyspace = targetKeyspace
		dest = targetDest
	}

	if dest == nil {
		if err := vschema.ErrorIfShardedF(keyspace, "CALL", errNotAllowWhenSharded); err != nil {
			return nil, err
		}
		dest = key.DestinationAnyShard{}
	}

	if !isSystemSchema {
		stmt.Name.Qualifier = sqlparser.NewIdentifierCS("")
	}
	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             sqlparser.String(stmt),
	}), nil
}

const errNotAllowWhenSharded = "CALL is not supported for sharded keyspace"
