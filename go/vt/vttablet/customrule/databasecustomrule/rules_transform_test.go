/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package databasecustomrule

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

	qr.AddTableCond("db1.table1")
	qr.AddTableCond("*.*")
	qr.AddTableCond("*.table")
	qr.AddTableCond("db3.*")

	qr.AddBindVarCond("b", false, true, rules.QREqual, "b")
	qr.AddBindVarCond("a", true, false, rules.QREqual, "a")

	return qr
}

func expectedJSONString() string {
	return `{"Description":"ruleDescription","Name":"ruleName","Priority":1000,"Status":"ACTIVE","RequestIP":".*","User":".*","Query":".*","QueryTemplate":"select * from t1 where a = :a and b = :b","LeadingComment":".*","TrailingComment":".*","Plans":["Insert","Select"],"FullyQualifiedTableNames":["db1.table1","*.*","*.table","db3.*"],"BindVarConds":[{"Name":"b","OnAbsent":false,"OnMismatch":true,"Operator":"==","Value":"b"},{"Name":"a","OnAbsent":true,"OnMismatch":false,"Operator":"==","Value":"a"}],"Action":"FAIL","ActionArgs":""}`
}

func expectedSQLString() string {
	return "INSERT INTO `mysql`.`wescale_plugin` (`name`, `description`, `priority`, `status`, `plans`, `fully_qualified_table_names`, `query_regex`, `query_template`, `request_ip_regex`, `user_regex`, `leading_comment_regex`, `trailing_comment_regex`, `bind_var_conds`, `action`, `action_args`) VALUES ('ruleName', 'ruleDescription', 1000, 'ACTIVE', '[\\\"Insert\\\",\\\"Select\\\"]', '[\\\"db1.table1\\\",\\\"*.*\\\",\\\"*.table\\\",\\\"db3.*\\\"]', '.*', 'select * from t1 where a = :a and b = :b', '.*', '.*', '.*', '.*', '[{\\\"Name\\\":\\\"b\\\",\\\"OnAbsent\\\":false,\\\"OnMismatch\\\":true,\\\"Operator\\\":\\\"==\\\",\\\"Value\\\":\\\"b\\\"},{\\\"Name\\\":\\\"a\\\",\\\"OnAbsent\\\":true,\\\"OnMismatch\\\":false,\\\"Operator\\\":\\\"==\\\",\\\"Value\\\":\\\"a\\\"}]', 'FAIL', '')"
}

func TestRule2Json(t *testing.T) {
	actualJSONString, err := json.Marshal(expectedRule())
	assert.NoError(t, err)
	fmt.Println(string(actualJSONString))
	assert.Equal(t, expectedJSONString(), string(actualJSONString))
}

func TestRule2SQL(t *testing.T) {
	controller := NewMockController()
	cr, _ := newDatabaseCustomRule(controller)

	qr := expectedRule()
	sql, err := cr.GenerateInsertStatement(qr)
	assert.NoError(t, err)
	fmt.Println(sql)
	assert.Equal(t, expectedSQLString(), sql)

	tableFields := []*querypb.Field{
		//{
		//	Name: "id",
		//	Type: sqltypes.Uint64,
		//}, {
		//	Name: "create_timestamp",
		//	Type: sqltypes.Timestamp,
		//}, {
		//	Name: "update_timestamp",
		//	Type: sqltypes.Timestamp,
		//},
		{
			Name: "name",
			Type: sqltypes.VarChar,
		}, {
			Name: "description",
			Type: sqltypes.Text,
		}, {
			Name: "priority",
			Type: sqltypes.Int32,
		}, {
			Name: "status",
			Type: sqltypes.VarChar,
		}, {
			Name: "plans",
			Type: sqltypes.Text,
		}, {
			Name: "fully_qualified_table_names",
			Type: sqltypes.Text,
		}, {
			Name: "query_regex",
			Type: sqltypes.Text,
		}, {
			Name: "query_template",
			Type: sqltypes.Text,
		}, {
			Name: "request_ip_regex",
			Type: sqltypes.VarChar,
		}, {
			Name: "user_regex",
			Type: sqltypes.VarChar,
		}, {
			Name: "leading_comment_regex",
			Type: sqltypes.Text,
		}, {
			Name: "trailing_comment_regex",
			Type: sqltypes.Text,
		}, {
			Name: "bind_var_conds",
			Type: sqltypes.Text,
		}, {
			Name: "action",
			Type: sqltypes.VarChar,
		}, {
			Name: "action_args",
			Type: sqltypes.Text,
		}}
	queryResult := &sqltypes.Result{
		Fields: tableFields,
		Rows: [][]sqltypes.Value{{
			//sqltypes.NewUint64(1),                                                                   // id
			//sqltypes.NewTimestamp(time.Now()),                                                       // create_timestamp
			//sqltypes.NewTimestamp(time.Now()),                                                       // update_timestamp
			sqltypes.NewVarChar("ruleName"),                                                         // name
			sqltypes.MakeTrusted(sqltypes.Text, []byte("ruleDescription")),                          // description
			sqltypes.NewInt32(1000),                                                                 // priority
			sqltypes.NewVarChar("ACTIVE"),                                                           // status
			sqltypes.MakeTrusted(sqltypes.Text, []byte(`["Insert","Select"]`)),                      // plans
			sqltypes.MakeTrusted(sqltypes.Text, []byte(`["db1.table1","*.*","*.table","db3.*"]`)),   // fully_qualified_table_names
			sqltypes.MakeTrusted(sqltypes.Text, []byte(".*")),                                       // query_regex
			sqltypes.MakeTrusted(sqltypes.Text, []byte("select * from t1 where a = :a and b = :b")), // query_template
			sqltypes.NewVarChar(".*"),                                                               // request_ip_regex
			sqltypes.NewVarChar(".*"),                                                               // user_regex
			sqltypes.MakeTrusted(sqltypes.Text, []byte(".*")),                                       // leading_comment_regex
			sqltypes.MakeTrusted(sqltypes.Text, []byte(".*")),                                       // trailing_comment_regex
			sqltypes.MakeTrusted(sqltypes.Text, []byte(`[{"Name":"b","OnAbsent":false,"OnMismatch":true,"Operator":"","Value":null},{"Name":"a","OnAbsent":true,"OnMismatch":false,"Operator":"","Value":null}]`)), // bind_var_conds
			sqltypes.NewVarChar("FAIL"),                     // action
			sqltypes.MakeTrusted(sqltypes.Text, []byte("")), // action_args
		}},
	}
	rule, err := queryResultToRule(queryResult.Named().Rows[0])
	assert.NoError(t, err)
	fmt.Println(rule)
	//todo filter: support bind_var_conds
	//assert.Equal(t, expectedRule(), rule)
}
