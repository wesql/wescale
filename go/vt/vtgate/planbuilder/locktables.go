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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

const (
	lockTablePrefix = "__locktable_"
)

// buildLockPlan plans lock tables statement.
func buildLockPlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	lockTables, ok := stmt.(*sqlparser.LockTables)
	if !ok {
		return nil, vterrors.VT13001("statement type unexpected, expect LockTables")
	}
	lockFuncs := buildLockFuncFromLockTables(lockTables, sqlparser.GetLock)
	plan, err := buildPlanForBypassWithLocks(stmt, reservedVars, vschema, lockFuncs)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// buildUnlockPlan plans lock tables statement.
func buildUnlockPlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	lockTables, ok := stmt.(*sqlparser.LockTables)
	if !ok {
		return nil, vterrors.VT13001("statement type unexpected, expect LockTables")
	}
	lockFuncs := buildLockFuncFromLockTables(lockTables, sqlparser.ReleaseLock)
	plan, err := buildPlanForBypassWithLocks(stmt, reservedVars, vschema, lockFuncs)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func buildLockFuncFromLockTables(lockTables *sqlparser.LockTables, lockType sqlparser.LockingFuncType) []*engine.SessionLock {
	lockFuncs := []*engine.SessionLock{}
	for _, lockTable := range lockTables.Tables {
		t, ok := lockTable.Table.(*sqlparser.AliasedTableExpr)
		if !ok {
			continue
		}
		tableName, ok := t.Expr.(sqlparser.TableName)
		if !ok {
			continue
		}
		lockFuncs = append(lockFuncs, &engine.SessionLock{
			Typ:  lockType,
			Name: tableName.Name.String(),
		})
	}
	return lockFuncs
}
