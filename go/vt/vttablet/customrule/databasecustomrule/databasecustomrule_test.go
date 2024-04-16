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

	defer func() {
		databaseCustomRuleDbName = "mysql"
		databaseCustomRuleTableName = "wescale_plugin"
	}()

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
