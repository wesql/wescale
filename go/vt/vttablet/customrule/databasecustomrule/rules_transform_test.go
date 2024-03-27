/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func expectedRule() *rules.Rule {
	qr := rules.NewQueryRule("ruleDescription", "ruleName", rules.QRFail)

	qr.SetStatus(rules.Active)
	qr.SetPriority(1000)

	qr.SetIPCond(".*")
	qr.SetUserCond(".*")
	qr.SetQueryCond(".*")
	qr.SetLeadingCommentCond(".*")
	qr.SetTrailingCommentCond(".*")

	qr.SetQueryTemplate("select * from t1 where a = :a and b = :b")
	qr.SetAction(rules.QRFail)
	qr.SetActionArgs("")

	qr.AddPlanCond(planbuilder.PlanInsert)
	qr.AddPlanCond(planbuilder.PlanSelect)

	qr.AddTableCond("table1")
	qr.AddTableCond("table2")

	qr.AddBindVarCond("b", false, true, rules.QRNoOp, nil)
	qr.AddBindVarCond("a", true, false, rules.QRNoOp, nil)

	return qr
}

func expectedJSONString() string {
	return `{"Description":"ruleDescription","Name":"ruleName","Priority":1000,"Status":"ACTIVE","RequestIP":".*","User":".*","Query":".*","QueryTemplate":"select * from t1 where a = :a and b = :b","LeadingComment":".*","TrailingComment":".*","Plans":["Insert","Select"],"TableNames":["table1","table2"],"BindVarConds":[{"Name":"b","OnAbsent":false,"Operator":""},{"Name":"a","OnAbsent":true,"Operator":""}],"Action":"FAIL","ActionArgs":""}`
}

func TestRule2Json(t *testing.T) {
	actualJSONString, err := json.Marshal(expectedRule())
	assert.NoError(t, err)
	assert.Equal(t, expectedJSONString(), string(actualJSONString))
}

func TestRule2SQL(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)

	qr := expectedRule()
	sql, err := cr.GenerateInsertStatement(qr)
	assert.NoError(t, err)
	fmt.Println(sql)
}
