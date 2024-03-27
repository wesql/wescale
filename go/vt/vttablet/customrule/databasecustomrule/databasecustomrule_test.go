/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestGetReloadSqlWithDefaultValues(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)

	expectedSQL := "SELECT * FROM mysql.wescale_plugin"
	actualSQL := cr.getReloadSQL()

	assert.Equal(t, expectedSQL, actualSQL)
}

func TestGetReloadSqlWithCustomValues(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)

	databaseCustomRuleDbName = "custom_db"
	databaseCustomRuleTableName = "custom_table"

	expectedSQL := "SELECT * FROM custom_db.custom_table"
	actualSQL := cr.getReloadSQL()

	assert.Equal(t, expectedSQL, actualSQL)
}

func NewMockController() *tabletservermock.Controller {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	qsc := tabletservermock.NewController()
	qsc.TS = ts
	return qsc
}

func TestDatabaseCustomRuleApplyRules(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)

	qr := &sqltypes.Result{}
	err := cr.applyRules(qr)

	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}
}

func NewMockResult() *sqltypes.Result {
	return &sqltypes.Result{}
}

/**
INSERT INTO mysql.wescale_plugin (
    name,
    description,
    priority,
    status,
    plans,
    fully_qualified_table_names,
    query_regex,
    query_template,
    request_ip_regex,
    user_regex,
    leading_comment_regex,
    trailing_comment_regex,
    bind_var_conds,
    action,
    action_args
) VALUES (
    'TestPlugin1',
    'This is a description for TestPlugin1',
    1000,
    'ACTIVE',
    'plan1;plan2',
    'db.table1;db.table2',
    '^SELECT.*FROM table1',
    'SELECT * FROM table1 WHERE id = ?',
    '^192\\.168\\.1\\.1$',
    '^user1$',
    '^-- This is a leading comment',
    '-- This is a trailing comment',
    'id > 10 AND name = "test"',
    'CONTINUE',
    'arg1'
);
*/

func TestApplyRulesWithEmptyResult(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)
	qr := NewMockResult()

	err := cr.applyRules(qr)

	assert.NoError(t, err)
}

func TestApplyRulesWithStoppedRule(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)
	cr.stop()
	qr := NewMockResult()

	err := cr.applyRules(qr)

	assert.NoError(t, err)
}

//
//func TestApplyRulesWithInvalidRule(t *testing.T) {
//	controller := NewMockController()
//	cr, _ := newDatabaseCustomRule(controller)
//	qr := NewMockResult()
//	qr.Rows = append(qr.Rows, []sqltypes.Value{
//		sqltypes.NewVarChar("name"),
//		sqltypes.NewVarChar("description"),
//		sqltypes.NewInt64(1000),
//		sqltypes.NewVarChar("invalid status"),
//		sqltypes.NewVarChar("[]"),
//		sqltypes.NewVarChar("[]"),
//		sqltypes.NewVarChar("query_regex"),
//		sqltypes.NewVarChar("query_template"),
//		sqltypes.NewVarChar("request_ip_regex"),
//		sqltypes.NewVarChar("user_regex"),
//		sqltypes.NewVarChar("leading_comment_regex"),
//		sqltypes.NewVarChar("trailing_comment_regex"),
//		sqltypes.NewVarChar("[]"),
//		sqltypes.NewVarChar("action"),
//		sqltypes.NewVarChar("action_args"),
//	})
//
//	err := cr.applyRules(qr)
//
//	assert.NoError(t, err)
//}
