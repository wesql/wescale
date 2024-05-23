package customrule

import (
	"fmt"
	"testing"

	"github.com/wesql/wescale/go/sqltypes"
	querypb "github.com/wesql/wescale/go/vt/proto/query"
	"github.com/wesql/wescale/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestUserInputStrArrayToArray(t *testing.T) {
	testCases := []struct {
		input       string
		expected    []string
		expectedErr bool
	}{
		{
			input:    `a,b`,
			expected: []string{"a", "b"},
		},
		{
			input:    `1,2`,
			expected: []string{"1", "2"},
		},
		{
			input:    `a b cde ;f g, h`,
			expected: []string{"abcde;fg", "h"},
		},
		{
			input:    ``,
			expected: []string{},
		},
		{
			input:    `,`,
			expected: []string{"", ""},
		},
		{
			input:    `,,`,
			expected: []string{"", "", ""},
		},
	}
	for _, tt := range testCases {
		got, err := UserInputStrArrayToArray(tt.input)

		if tt.expectedErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			for i, v := range got {
				sv := v.(string)
				assert.Equal(t, sv, tt.expected[i])
			}
		}
	}
}

func TestQueryResultToCreateFilter(t *testing.T) {
	tableFields := []*querypb.Field{
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

	testCases := []struct {
		name                     string
		desc                     string
		priority                 int32
		status                   string
		plans                    string
		fullyQualifiedTableNames string
		queryRegex               string
		queryTemplate            string
		requestIPRegex           string
		userRegex                string
		leadingCommentRegex      string
		trailingCommentRegex     string
		bindVarConds             string
		action                   string
		actionArgs               string

		expectedErr bool
	}{
		{
			name:                     "test1",
			desc:                     "test1 desc",
			priority:                 10,
			status:                   "ACTIVE",
			plans:                    `["Insert","Select"]`,
			fullyQualifiedTableNames: `["db1.table1","*.*","*.table","db3.*"]`,
			queryRegex:               `.*`,
			queryTemplate:            `select * from t1 where a = :a and b = :b`,
			requestIPRegex:           `.*`,
			userRegex:                `.*`,
			leadingCommentRegex:      `.*`,
			trailingCommentRegex:     `.*`,
			bindVarConds:             ``,
			action:                   "FAIL",
			actionArgs:               ``,
		},
		{
			name:                     "test1",
			desc:                     "test1 desc",
			priority:                 10,
			status:                   "ACTIVE",
			plans:                    `["Insert","Select"]`,
			fullyQualifiedTableNames: `["db1.table1","*.*","*.table","db3.*"]`,
			queryRegex:               `.*`,
			queryTemplate:            `select * from t1 where a = :a and b = :b`,
			requestIPRegex:           `127.0.0.\.*`,
			userRegex:                `.*`,
			leadingCommentRegex:      `.*`,
			trailingCommentRegex:     `.*`,
			bindVarConds:             ``,
			action:                   "FAIL",
			actionArgs:               ``,
		},
	}

	for _, tt := range testCases {
		queryResult := &sqltypes.Result{
			Fields: tableFields,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarChar(tt.name),                                             // name
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.desc)),                     // description
				sqltypes.NewInt32(tt.priority),                                           // priority
				sqltypes.NewVarChar(tt.status),                                           // status
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.plans)),                    // plans
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.fullyQualifiedTableNames)), // fully_qualified_table_names
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.queryRegex)),               // query_regex
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.queryTemplate)),            // query_template
				sqltypes.NewVarChar(tt.requestIPRegex),                                   // request_ip_regex
				sqltypes.NewVarChar(tt.userRegex),                                        // user_regex
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.leadingCommentRegex)),      // leading_comment_regex
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.trailingCommentRegex)),     // trailing_comment_regex
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.bindVarConds)),             // bind_var_conds
				sqltypes.NewVarChar(tt.action),                                           // action
				sqltypes.MakeTrusted(sqltypes.Text, []byte(tt.actionArgs)),               // action_args
			}},
		}
		filter, err := QueryResultToCreateFilter(queryResult.Named().Rows[0])
		if tt.expectedErr {
			assert.NotNil(t, err)
		}
		assert.Nil(t, err)
		fmt.Printf("%s\n", sqlparser.String(filter))
	}
}
