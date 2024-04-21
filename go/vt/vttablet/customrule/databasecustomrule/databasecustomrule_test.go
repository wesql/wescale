/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"testing"

	"vitess.io/vitess/go/vt/vttablet/customrule"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestGetReloadSqlWithDefaultValues(t *testing.T) {
	expectedSQL := "SELECT * FROM mysql.wescale_plugin"
	actualSQL := customrule.GetSelectAllSQL()

	assert.Equal(t, expectedSQL, actualSQL)
}

func TestGetReloadSqlWithCustomValues(t *testing.T) {
	customrule.DatabaseCustomRuleDbName = "custom_db"
	customrule.DatabaseCustomRuleTableName = "custom_table"

	defer func() {
		customrule.DatabaseCustomRuleDbName = "mysql"
		customrule.DatabaseCustomRuleTableName = "wescale_plugin"
	}()

	expectedSQL := "SELECT * FROM custom_db.custom_table"
	actualSQL := customrule.GetSelectAllSQL()

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
