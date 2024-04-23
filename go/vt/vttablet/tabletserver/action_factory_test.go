package tabletserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func TestGetActionList_NoRules(t *testing.T) {
	qrs := &rules.Rules{}
	actionList := GetActionList(qrs, "", "", nil, sqlparser.MarginComments{})
	assert.NotNil(t, actionList)
	assert.Equal(t, 0, len(actionList))
}

func TestGetActionList_MatchingRule(t *testing.T) {
	rule := rules.NewActiveQueryRule("test_rule", "test_rule", rules.QRFail)
	qrs := rules.New()
	qrs.Add(rule)
	actionList := GetActionList(qrs, "", "", nil, sqlparser.MarginComments{})
	assert.Equal(t, 1, len(actionList))
	assert.NotNil(t, actionList)
	assert.IsType(t, &FailAction{}, actionList[0])
}

func TestGetActionList_NonMatchingRule(t *testing.T) {
	rule := rules.NewActiveQueryRule("test_rule", "test_rule", rules.QRFail)
	rule.SetIPCond("1.1.1.1")
	qrs := rules.New()
	qrs.Add(rule)
	actionList := GetActionList(qrs, "", "", nil, sqlparser.MarginComments{})
	assert.Equal(t, 0, len(actionList))
}

func TestCreateActionInstance(t *testing.T) {

	cclRule := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRConcurrencyControl)
	cclRule.SetActionArgs(`max_queue_size=1; max_concurrency=1`)

	type args struct {
		action rules.Action
		rule   *rules.Rule
	}
	tests := []struct {
		name    string
		args    args
		want    ActionInterface
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "QRContinue",
			args: args{
				action: rules.QRContinue,
				rule:   rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRContinue),
			},
			want:    &ContinueAction{Rule: rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
			wantErr: assert.NoError,
		},
		{
			name: "QRFail",
			args: args{
				action: rules.QRFail,
				rule:   rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail),
			},
			want:    &FailAction{Rule: rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail), Action: rules.QRFail},
			wantErr: assert.NoError,
		},
		{
			name: "QRFailRetry",
			args: args{
				action: rules.QRFailRetry,
				rule:   rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFailRetry),
			},
			want:    &FailRetryAction{Rule: rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFailRetry), Action: rules.QRFailRetry},
			wantErr: assert.NoError,
		},
		{
			name: "QRBuffer",
			args: args{
				action: rules.QRBuffer,
				rule:   rules.NewActiveBufferedTableQueryRule(nil, "test_rule", "desc"),
			},
			want:    &BufferAction{Rule: rules.NewActiveBufferedTableQueryRule(nil, "test_rule", "desc"), Action: rules.QRBuffer},
			wantErr: assert.NoError,
		},
		{
			name: "QRConcurrencyControl",
			args: args{
				action: rules.QRConcurrencyControl,
				rule:   cclRule,
			},
			want:    &ConcurrencyControlAction{Rule: cclRule, Action: rules.QRConcurrencyControl, Args: &ConcurrencyControlActionArgs{MaxConcurrency: 1, MaxQueueSize: 1}},
			wantErr: assert.NoError,
		},
		{
			name: "unknown action",
			args: args{
				action: rules.Action(100),
				rule:   rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRContinue),
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateActionInstance(tt.args.action, tt.args.rule)
			if !tt.wantErr(t, err, fmt.Sprintf("CreateActionInstance(%v, %v)", tt.args.action, tt.args.rule)) {
				return
			}
			assert.Equalf(t, tt.want, got, "CreateActionInstance(%v, %v)", tt.args.action, tt.args.rule)
		})
	}
}

func TestCreateContinueAction(t *testing.T) {
	tests := []struct {
		name string
		want ActionInterface
	}{
		{
			name: "CreateContinueAction",
			want: &ContinueAction{Rule: &rules.Rule{Name: "continue_action", Priority: rules.DefaultPriority}, Action: rules.QRContinue},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, CreateContinueAction(), "CreateContinueAction()")
		})
	}
}

func Test_sortAction(t *testing.T) {
	r1 := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail)
	r2 := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail)
	r3 := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail)

	a1 := &FailAction{Rule: r1, Action: rules.QRFail}
	a2 := &FailAction{Rule: r2, Action: rules.QRFail}
	a3 := &FailAction{Rule: r3, Action: rules.QRFail}

	a1.GetRule().SetPriority(3)
	a2.GetRule().SetPriority(2)
	a3.GetRule().SetPriority(1)

	actionList := []ActionInterface{a1, a2, a3}
	sortAction(actionList)

	assert.Equal(t, a3, actionList[0])
	assert.Equal(t, a2, actionList[1])
	assert.Equal(t, a1, actionList[2])
}
