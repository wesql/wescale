/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package planbuilder

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type fkStrategy int

const (
	fkAllow fkStrategy = iota
	fkDisallow
)

var fkStrategyMap = map[string]fkStrategy{
	"allow":    fkAllow,
	"disallow": fkDisallow,
}

type fkContraint struct {
	found bool
}

func (fk *fkContraint) FkWalk(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable,
		*sqlparser.TableSpec, *sqlparser.AddConstraintDefinition, *sqlparser.ConstraintDefinition:
		return true, nil
	case *sqlparser.ForeignKeyDefinition:
		fk.found = true
	}
	return false, nil
}

// buildGeneralDDLPlan builds a general DDL plan, which can be either normal DDL or online DDL.
// The two behave completely differently, and have two very different primitives.
// We want to be able to dynamically choose between normal/online plans according to Session settings.
// However, due to caching of plans, we're unable to make that choice right now. In this function we don't have
// a session context. It's only when we Execute() the primitive that we have that context.
// This is why we return a compound primitive (DDL) which contains fully populated primitives (Send & OnlineDDL),
// and which chooses which of the two to invoke at runtime.
func buildGeneralDDLPlan(sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	var err error
	normalDDLPlan, onlineDDLPlan, err := buildDDLPlans(sql, ddlStatement, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}

	if ddlStatement.IsTemporary() {
		err := vschema.ErrorIfShardedF(normalDDLPlan.Keyspace, "temporary table", "Temporary table not supported in sharded database %s", normalDDLPlan.Keyspace.Name)
		if err != nil {
			return nil, err
		}
		onlineDDLPlan = nil // emptying this so it does not accidentally gets used somewhere
	}

	normalDeclarativeDDL := &engine.DirectDeclarativeDDL{}
	if createTable, ok := ddlStatement.(*sqlparser.CreateTable); ok && onlineDDLPlan.DDLStrategySetting.Strategy == schema.DDLStrategyDirect && vschema.GetSession().EnableDeclarativeDDL {
		// init primitive for direct declarative DDL
		cursor, ok := vschema.(engine.VCursor)
		if !ok {
			return nil, fmt.Errorf("build direct declarative ddl plan failed: vschema does not implement VCursor interface")
		}
		normalDeclarativeDDL, err = engine.InitDirectDeclarativeDDL(context.Background(), createTable, cursor)
		if err != nil {
			return nil, err
		}
	}

	eddl := &engine.DDL{
		Keyspace:             normalDDLPlan.Keyspace,
		SQL:                  normalDDLPlan.Query,
		DDL:                  ddlStatement,
		NormalDDL:            normalDDLPlan,
		OnlineDDL:            onlineDDLPlan,
		NormalDeclarativeDDL: normalDeclarativeDDL,

		DirectDDLEnabled: enableDirectDDL,
		OnlineDDLEnabled: enableOnlineDDL,

		CreateTempTable: ddlStatement.IsTemporary(),
	}
	affectedTables := make([]string, 0)
	for _, tbl := range ddlStatement.AffectedTables() {
		fullTableName := sqlparser.String(tbl)
		affectedTables = append(affectedTables, fullTableName)
	}

	return newPlanResult(eddl, affectedTables...), nil
}

func buildByPassDDLPlan(sql string, vschema plancontext.VSchema) (*planResult, error) {
	destination := vschema.Destination()
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil && err.Error() != vterrors.VT09005().Error() {
		return nil, err
	}
	send := buildNormalDDLPlan(keyspace, destination, sql)
	return newPlanResult(send), nil
}

func buildNormalDDLPlan(keyspace *vindexes.Keyspace, destination key.Destination, sql string) *engine.Send {
	send := &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql,
	}
	return send
}

func buildDDLPlans(sql string, ddlStatement sqlparser.DDLStatement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*engine.Send, *engine.OnlineDDL, error) {
	destination := vschema.Destination()
	if destination == nil {
		destination = key.DestinationAllShards{}
	}
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil && err.Error() != vterrors.VT09005().Error() {
		return nil, nil, err
	}

	switch ddlStatement.(type) {
	case *sqlparser.AlterTable, *sqlparser.CreateTable, *sqlparser.TruncateTable:
		err = checkFKError(vschema, ddlStatement)
		if err != nil {
			return nil, nil, err
		}
	case *sqlparser.CreateView:
	case *sqlparser.AlterView:
	case *sqlparser.DropView:
	case *sqlparser.DropTable:
	case *sqlparser.RenameTable:
	default:
		return nil, nil, vterrors.VT13001(fmt.Sprintf("unexpected DDL statement type: %T", ddlStatement))
	}

	query := sql
	// If the query is fully parsed, generate the query from the ast. Otherwise, use the original query
	if ddlStatement.IsFullyParsed() {
		query = sqlparser.String(ddlStatement)
	}

	normalDDL := buildNormalDDLPlan(keyspace, destination, sql)

	ddlStrategySetting, err := schema.ParseDDLStrategy(vschema.GetSession().GetDDLStrategy())
	if err != nil {
		return nil, nil, err
	}

	onlineDDL := &engine.OnlineDDL{
		Keyspace:           keyspace,
		TargetDestination:  destination,
		DDL:                ddlStatement,
		SQL:                query,
		DDLStrategySetting: ddlStrategySetting,
	}

	return normalDDL, onlineDDL, nil
}

func checkFKError(vschema plancontext.VSchema, ddlStatement sqlparser.DDLStatement) error {
	if fkStrategyMap[vschema.ForeignKeyMode()] == fkDisallow {
		fk := &fkContraint{}
		_ = sqlparser.Walk(fk.FkWalk, ddlStatement)
		if fk.found {
			return vterrors.VT10001()
		}
	}
	return nil
}
