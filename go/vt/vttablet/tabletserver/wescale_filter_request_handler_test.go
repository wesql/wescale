package tabletserver

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestConvertUserInputToTOML(t *testing.T) {
	test := []struct {
		input    string
		expected string
	}{
		{
			input:    "a=1;b=2",
			expected: "a=1\nb=2",
		},
		{
			input:    "a=1;b=[2,3,4,\"5\"];c=6",
			expected: "a=1\nb=[2,3,4,\"5\"]\nc=6",
		},
		{
			input:    "a=1;b=\"here is a ;\";c=6",
			expected: "a=1\nb=\"here is a ;\"\nc=6",
		},
	}
	for _, tt := range test {
		got := ConvertUserInputToTOML(tt.input)
		assert.Equal(t, tt.expected, got)
	}
}

func TestAlterRuleInfo(t *testing.T) {
	testCases := []struct {
		alter            *sqlparser.AlterWescaleFilter
		originRuleInfo   map[string]any
		expectedRuleInfo map[string]any
		expectedErr      bool
	}{
		{
			alter: &sqlparser.AlterWescaleFilter{NewName: "-1", Description: "-1", Priority: "-1", SetPriority: false,
				Status: "-1", Pattern: &sqlparser.WescaleFilterPattern{Plans: "-1", FullyQualifiedTableNames: "-1", QueryRegex: "-1",
					QueryTemplate: "-1", RequestIPRegex: "-1", UserRegex: "-1", LeadingCommentRegex: "-1", TrailingCommentRegex: "-1",
					BindVarConds: "-1"}, Action: &sqlparser.WescaleFilterAction{Action: "-1", ActionArgs: "-1"}},
			originRuleInfo: map[string]any{"Name": "p1", "Description": "nothing", "Priority": "1000",
				"Query": "", "QueryTemplate": "", "RequestIP": "", "User": "", "LeadingComment": "", "TrailingComment": "",
				"Plans": []string{"insert"}, "FullyQualifiedTableNames": []string{"d1.t1"},
				"Status": "ACTIVE", "Action": "FAIL", "ActionArgs": ""},
			expectedRuleInfo: map[string]any{"Name": "p1", "Description": "nothing", "Priority": "1000",
				"Query": "", "QueryTemplate": "", "RequestIP": "", "User": "", "LeadingComment": "", "TrailingComment": "",
				"Plans": []string{"insert"}, "FullyQualifiedTableNames": []string{"d1.t1"},
				"Status": "ACTIVE", "Action": "FAIL", "ActionArgs": ""},
		},
		{
			alter: &sqlparser.AlterWescaleFilter{NewName: "p2", Description: "new desc", Priority: "1", SetPriority: true,
				Status: "inactive", Pattern: &sqlparser.WescaleFilterPattern{Plans: "insert ,select", FullyQualifiedTableNames: "d1.t1, d2.t2", QueryRegex: ".*",
					QueryTemplate: ".*", RequestIPRegex: ".*", UserRegex: ".*", LeadingCommentRegex: ".*", TrailingCommentRegex: ".*",
					BindVarConds: "-1"}, Action: &sqlparser.WescaleFilterAction{Action: "concurrency_control", ActionArgs: "a=1;b=1"}},
			originRuleInfo: map[string]any{"Name": "p1", "Description": "nothing", "Priority": "1000",
				"Query": "", "QueryTemplate": "", "RequestIP": "", "User": "", "LeadingComment": "", "TrailingComment": "",
				"Plans": []string{"insert"}, "FullyQualifiedTableNames": []string{"d1.t1"},
				"Status": "ACTIVE", "Action": "FAIL", "ActionArgs": ""},
			expectedRuleInfo: map[string]any{"Name": "p2", "Description": "new desc", "Priority": "1",
				"Query": ".*", "QueryTemplate": ".*", "RequestIP": ".*", "User": ".*", "LeadingComment": ".*", "TrailingComment": ".*",
				"Plans": []any{"insert", "select"}, "FullyQualifiedTableNames": []any{"d1.t1", "d2.t2"},
				"Status": "inactive", "Action": "concurrency_control", "ActionArgs": "a=1;b=1"},
		},
		{ // set priority 1000 -> -1
			alter: &sqlparser.AlterWescaleFilter{NewName: "-1", Description: "-1", Priority: "-1", SetPriority: true,
				Status: "-1", Pattern: &sqlparser.WescaleFilterPattern{Plans: "-1", FullyQualifiedTableNames: "-1", QueryRegex: "-1",
					QueryTemplate: "-1", RequestIPRegex: "-1", UserRegex: "-1", LeadingCommentRegex: "-1", TrailingCommentRegex: "-1",
					BindVarConds: "-1"}, Action: &sqlparser.WescaleFilterAction{Action: "-1", ActionArgs: "-1"}},
			expectedErr: true,
		},
		{ // wrong format priority
			alter: &sqlparser.AlterWescaleFilter{NewName: "-1", Description: "-1", Priority: "wrong format", SetPriority: true,
				Status: "-1", Pattern: &sqlparser.WescaleFilterPattern{Plans: "-1", FullyQualifiedTableNames: "-1", QueryRegex: "-1",
					QueryTemplate: "-1", RequestIPRegex: "-1", UserRegex: "-1", LeadingCommentRegex: "-1", TrailingCommentRegex: "-1",
					BindVarConds: "-1"}, Action: &sqlparser.WescaleFilterAction{Action: "-1", ActionArgs: "-1"}},
			expectedErr: true,
		},
		{ // invalid ccl args
			alter: &sqlparser.AlterWescaleFilter{NewName: "-1", Description: "-1", Priority: "-1", SetPriority: false,
				Status: "-1", Pattern: &sqlparser.WescaleFilterPattern{Plans: "-1", FullyQualifiedTableNames: "-1", QueryRegex: "-1",
					QueryTemplate: "-1", RequestIPRegex: "-1", UserRegex: "-1", LeadingCommentRegex: "-1", TrailingCommentRegex: "-1",
					BindVarConds: "-1"}, Action: &sqlparser.WescaleFilterAction{Action: "-1", ActionArgs: "max_queue_size=-1"}},
			originRuleInfo: map[string]any{"Name": "p1", "Description": "nothing", "Priority": "1000",
				"Query": "", "QueryTemplate": "", "RequestIP": "", "User": "", "LeadingComment": "", "TrailingComment": "",
				"Plans": []string{"insert"}, "FullyQualifiedTableNames": []string{"d1.t1"},
				"Status": "ACTIVE", "Action": "concurrency_control", "ActionArgs": ""},
			expectedErr: true,
		},
	}
	for _, tt := range testCases {
		err := AlterRuleInfo(tt.originRuleInfo, tt.alter)
		if tt.expectedErr {
			assert.Error(t, err)
		} else {
			//assert.NoError(t, err)
			//for k, v := range tt.originRuleInfo {
			//	assert.Equal(t, tt.expectedRuleInfo[k], v)
			//}
			assert.True(t, reflect.DeepEqual(tt.expectedRuleInfo, tt.originRuleInfo))
		}
	}
}
