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
	"fmt"
	"sort"

	"github.com/wesql/wescale/go/vt/vtgate/planbuilder/plancontext"

	"github.com/wesql/wescale/go/sqltypes"
	querypb "github.com/wesql/wescale/go/vt/proto/query"

	"github.com/wesql/wescale/go/vt/vterrors"

	"github.com/wesql/wescale/go/vt/key"
	"github.com/wesql/wescale/go/vt/sqlparser"
	"github.com/wesql/wescale/go/vt/vtgate/engine"
	"github.com/wesql/wescale/go/vt/vtgate/semantics"
	"github.com/wesql/wescale/go/vt/vtgate/vindexes"
)

const (
	// V3 is also the default planner
	V3 = querypb.ExecuteOptions_V3
	// Gen4 uses the default Gen4 planner, which is the greedy planner
	Gen4 = querypb.ExecuteOptions_Gen4
	// Gen4GreedyOnly uses only the faster greedy planner
	Gen4GreedyOnly = querypb.ExecuteOptions_Gen4Greedy
	// Gen4Left2Right tries to emulate the V3 planner by only joining plans in the order they are listed in the FROM-clause
	Gen4Left2Right = querypb.ExecuteOptions_Gen4Left2Right
	// Gen4WithFallback first attempts to use the Gen4 planner, and if that fails, uses the V3 planner instead
	Gen4WithFallback = querypb.ExecuteOptions_Gen4WithFallback
	// Gen4CompareV3 executes queries on both Gen4 and V3 to compare their results.
	Gen4CompareV3 = querypb.ExecuteOptions_Gen4CompareV3
)

var (
	plannerVersions = []plancontext.PlannerVersion{V3, Gen4, Gen4GreedyOnly, Gen4Left2Right, Gen4WithFallback, Gen4CompareV3}
)

type (
	truncater interface {
		SetTruncateColumnCount(int)
	}

	planResult struct {
		primitive engine.Primitive
		tables    []string
	}

	stmtPlanner func(sqlparser.Statement, *sqlparser.ReservedVars, plancontext.VSchema) (*planResult, error)
)

func newPlanResult(prim engine.Primitive, tablesUsed ...string) *planResult {
	return &planResult{primitive: prim, tables: tablesUsed}
}

func singleTable(ks, tbl string) string {
	return fmt.Sprintf("%s.%s", ks, tbl)
}

func tablesFromSemantics(semTable *semantics.SemTable) []string {
	tables := make(map[string]any, len(semTable.Tables))
	for _, info := range semTable.Tables {
		vindexTable := info.GetVindexTable()
		if vindexTable == nil {
			continue
		}
		tables[vindexTable.String()] = nil
	}

	names := make([]string, 0, len(tables))
	for tbl := range tables {
		names = append(names, tbl)
	}
	sort.Strings(names)
	return names
}

// TestBuilder builds a plan for a query based on the specified vschema.
// This method is only used from tests
func TestBuilder(query string, vschema plancontext.VSchema, keyspace string) (*engine.Plan, error) {
	stmt, reserved, err := sqlparser.Parse2(query)
	if err != nil {
		return nil, err
	}
	result, err := sqlparser.RewriteAST(stmt, keyspace, sqlparser.SQLSelectLimitUnset, "", nil, vschema)
	if err != nil {
		return nil, err
	}

	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	return BuildFromStmt(query, result.AST, reservedVars, vschema, result.BindVarNeeds, true, true)
}

// BuildFromStmt builds a plan based on the AST provided.
func BuildFromStmt(query string, stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, bindVarNeeds *sqlparser.BindVarNeeds, enableOnlineDDL, enableDirectDDL bool) (*engine.Plan, error) {
	planResult, err := createInstructionFor(query, stmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}

	var primitive engine.Primitive
	var tablesUsed []string
	if planResult != nil {
		primitive = planResult.primitive
		tablesUsed = planResult.tables
	}
	plan := &engine.Plan{
		Type:         sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: primitive,
		BindVarNeeds: bindVarNeeds,
		TablesUsed:   tablesUsed,
	}
	return plan, nil
}

func createInstructionFor(query string, stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return gen4SelectStmtPlanner(query, stmt, reservedVars, vschema)
	case *sqlparser.Insert:
		return buildPlanForBypass(stmt, reservedVars, vschema)
	case *sqlparser.Update:
		return buildPlanForBypass(stmt, reservedVars, vschema)
	case *sqlparser.Delete:
		return buildPlanForBypass(stmt, reservedVars, vschema)
	case *sqlparser.Union:
		return gen4SelectStmtPlanner(query, stmt, reservedVars, vschema)
	case sqlparser.DDLStatement:
		return buildGeneralDDLPlan(query, stmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case *sqlparser.AlterMigration:
		return buildAlterMigrationPlan(query, vschema, enableOnlineDDL)
	case *sqlparser.AlterDMLJob:
		return buildAlterDMLJobPlan(query, vschema)
	case *sqlparser.RevertMigration:
		return buildRevertMigrationPlan(query, stmt, vschema, enableOnlineDDL)
	case *sqlparser.ShowMigrationLogs:
		return buildShowMigrationLogsPlan(query, vschema, enableOnlineDDL)
	case *sqlparser.ShowThrottledApps:
		return buildShowThrottledAppsPlan(query, vschema)
	case *sqlparser.ShowThrottlerStatus:
		return buildShowThrottlerStatusPlan(query, vschema)
	case *sqlparser.AlterVschema:
		return buildVSchemaDDLPlan(stmt, vschema)
	case *sqlparser.Use:
		return buildUsePlan(stmt)
	case sqlparser.Explain:
		return buildExplainPlan(stmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case *sqlparser.VExplainStmt:
		return buildVExplainPlan(stmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	case *sqlparser.OtherRead, *sqlparser.OtherAdmin, *sqlparser.CheckTable, *sqlparser.Kill:
		// will send directly to vtttablet
		return buildOtherReadAndAdmin(query, vschema)
	case *sqlparser.Set:
		return buildSetPlan(stmt, vschema)
	case *sqlparser.Load:
		return buildLoadPlan(query, vschema)
	case sqlparser.DBDDLStatement:
		return buildDBDDLPlan(stmt, reservedVars, vschema)
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint, *sqlparser.SRollback, *sqlparser.Release:
		// Empty by design. Not executed by a plan
		return nil, nil
	case *sqlparser.Show:
		return buildShowPlan(query, stmt, reservedVars, vschema)
	case *sqlparser.LockTables:
		// will not be executed, Lock Tables statement is ignored
		return buildLockPlan(stmt, reservedVars, vschema)
	case *sqlparser.UnlockTables:
		// will not be executed, UnLock Tables statement is ignored
		return buildUnlockPlan(stmt, reservedVars, vschema)
	case *sqlparser.Flush:
		return buildFlushPlan(stmt, vschema)
	case *sqlparser.CallProc:
		return buildCallProcPlan(stmt, vschema)
	case *sqlparser.Stream:
		return buildStreamPlan(stmt, vschema)
	case *sqlparser.VStream:
		return buildVStreamPlan(stmt, vschema)
	case *sqlparser.Reload:
		return buildReloadPlan(stmt, vschema)

	case *sqlparser.CommentOnly:
		// There is only a comment in the input.
		// This is essentially a No-op
		return newPlanResult(engine.NewRowsPrimitive(nil, nil)), nil
	case *sqlparser.CreateWescaleFilter, *sqlparser.AlterWescaleFilter, *sqlparser.DropWescaleFilter, *sqlparser.ShowWescaleFilter:
		return buildWescaleFilterPlan(query, vschema)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unexpected statement type: %T", stmt))
}

func buildDBDDLPlan(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	dbDDLstmt := stmt.(sqlparser.DBDDLStatement)
	ksName := dbDDLstmt.GetDatabaseName()
	if ksName == "" {
		ks, err := vschema.DefaultKeyspace()
		if err != nil {
			return nil, err
		}
		ksName = ks.Name
	}
	ksExists := vschema.KeyspaceExists(ksName)

	switch dbDDL := dbDDLstmt.(type) {
	case *sqlparser.DropDatabase:
		if dbDDL.IfExists && !ksExists {
			return newPlanResult(engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0))), nil
		}
		if !ksExists {
			return nil, vterrors.VT05001(ksName)
		}
		return newPlanResult(engine.NewDBDDL(ksName, false, queryTimeout(dbDDL.Comments.Directives()))), nil
	case *sqlparser.AlterDatabase:
		if !ksExists {
			return nil, vterrors.VT05002(ksName)
		}
		return nil, vterrors.VT12001("ALTER DATABASE")
	case *sqlparser.CreateDatabase:
		if dbDDL.IfNotExists && ksExists {
			return newPlanResult(engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0))), nil
		}
		if !dbDDL.IfNotExists && ksExists {
			return nil, vterrors.VT06001(ksName)
		}
		return newPlanResult(engine.NewDBDDL(ksName, true, queryTimeout(dbDDL.Comments.Directives()))), nil
	}
	return nil, vterrors.VT13001(fmt.Sprintf("database DDL not recognized: %s", sqlparser.String(dbDDLstmt)))
}

func buildLoadPlan(query string, vschema plancontext.VSchema) (*planResult, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}

	destination := vschema.Destination()
	if destination == nil {
		if err := vschema.ErrorIfShardedF(keyspace, "LOAD", "LOAD is not supported on sharded keyspace"); err != nil {
			return nil, err
		}
		destination = key.DestinationAnyShard{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             query,
		IsDML:             true,
		SingleShardOnly:   true,
	}), nil
}

func buildVSchemaDDLPlan(stmt *sqlparser.AlterVschema, vschema plancontext.VSchema) (*planResult, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	return newPlanResult(&engine.AlterVSchema{
		Keyspace:        keyspace,
		AlterVschemaDDL: stmt,
	}, singleTable(keyspace.Name, stmt.Table.Name.String())), nil
}

func buildFlushPlan(stmt *sqlparser.Flush, vschema plancontext.VSchema) (*planResult, error) {
	if len(stmt.TableNames) == 0 {
		return buildFlushOptions(stmt, vschema)
	}
	return buildFlushTables(stmt, vschema)
}

func buildFlushOptions(stmt *sqlparser.Flush, vschema plancontext.VSchema) (*planResult, error) {
	dest, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if dest == nil {
		dest = key.DestinationAllShards{}
	}
	tc := &tableCollector{}
	for _, tbl := range stmt.TableNames {
		tc.addASTTable(keyspace.Name, tbl)
	}

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             sqlparser.String(stmt),
		IsDML:             false,
		SingleShardOnly:   false,
	}, tc.getTables()...), nil
}

func buildFlushTables(stmt *sqlparser.Flush, vschema plancontext.VSchema) (*planResult, error) {
	tc := &tableCollector{}
	type sendDest struct {
		ks   *vindexes.Keyspace
		dest key.Destination
	}

	dest := vschema.Destination()
	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	tablesMap := make(map[sendDest]sqlparser.TableNames)
	var keys []sendDest
	for i, tab := range stmt.TableNames {
		var ksTab *vindexes.Keyspace
		var table *vindexes.Table
		var err error

		table, _, _, _, _, err = vschema.FindTableOrVindex(tab)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, vindexes.NotFoundError{TableName: tab.Name.String()}
		}
		tc.addTable(table.Keyspace.Name, table.Name.String())
		ksTab = table.Keyspace
		stmt.TableNames[i] = sqlparser.TableName{
			Name: table.Name,
		}

		key := sendDest{ksTab, dest}
		tables, isAvail := tablesMap[key]
		if !isAvail {
			keys = append(keys, key)
		}
		tables = append(tables, stmt.TableNames[i])
		tablesMap[key] = tables
	}

	if len(tablesMap) == 1 {
		for sendDest, tables := range tablesMap {
			return newPlanResult(&engine.Send{
				Keyspace:          sendDest.ks,
				TargetDestination: sendDest.dest,
				Query:             sqlparser.String(newFlushStmt(stmt, tables)),
			}, tc.getTables()...), nil
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].ks.Name < keys[j].ks.Name
	})

	var sources []engine.Primitive
	for _, sendDest := range keys {
		plan := &engine.Send{
			Keyspace:          sendDest.ks,
			TargetDestination: sendDest.dest,
			Query:             sqlparser.String(newFlushStmt(stmt, tablesMap[sendDest])),
		}
		sources = append(sources, plan)
	}
	return newPlanResult(engine.NewConcatenate(sources, nil), tc.getTables()...), nil
}

type tableCollector struct {
	tables map[string]any
}

func (tc *tableCollector) addTable(ks, tbl string) {
	if tc.tables == nil {
		tc.tables = map[string]any{}
	}
	tc.tables[fmt.Sprintf("%s.%s", ks, tbl)] = nil
}

func (tc *tableCollector) addASTTable(ks string, tbl sqlparser.TableName) {
	tc.addTable(ks, tbl.Name.String())
}

func (tc *tableCollector) getTables() []string {
	tableNames := make([]string, 0, len(tc.tables))
	for tbl := range tc.tables {
		tableNames = append(tableNames, tbl)
	}

	sort.Strings(tableNames)
	return tableNames
}

func (tc *tableCollector) addVindexTable(t *vindexes.Table) {
	if t == nil {
		return
	}
	ks, tbl := "", t.Name.String()
	if t.Keyspace != nil {
		ks = t.Keyspace.Name
	}
	tc.addTable(ks, tbl)
}

func (tc *tableCollector) addAllTables(tables []string) {
	if tc.tables == nil {
		tc.tables = map[string]any{}
	}
	for _, tbl := range tables {
		tc.tables[tbl] = nil
	}
}

func newFlushStmt(stmt *sqlparser.Flush, tables sqlparser.TableNames) *sqlparser.Flush {
	return &sqlparser.Flush{
		IsLocal:    stmt.IsLocal,
		TableNames: tables,
		WithLock:   stmt.WithLock,
		ForExport:  stmt.ForExport,
	}
}

func buildReloadPlan(stmt *sqlparser.Reload, vschema plancontext.VSchema) (*planResult, error) {
	switch stmt.Type {
	case sqlparser.ReloadUsers:
		return newPlanResult(&engine.ReloadExec{ReloadType: stmt.Type}), nil
	case sqlparser.ReloadPrivileges:
		//send := &engine.Send{
		//	TargetDestination: vschema.Destination(),
		//	Query:             sqlparser.String(stmt),
		//	SingleShardOnly:   false,
		//}
		//return newPlanResult(send), nil
		return newPlanResult(&engine.ReloadExec{ReloadType: stmt.Type}), nil
	}
	return nil, vterrors.VT13001(fmt.Sprintf("unexpected statement type: %T", stmt))
}
