package tabletserver

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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
	rule := rules.NewQueryRule("test_rule", "test_rule", rules.QRContinue)
	qrs := rules.New()
	qrs.Add(rule)
	actionList := GetActionList(qrs, "", "", nil, sqlparser.MarginComments{})
	assert.Equal(t, 1, len(actionList))
	assert.IsType(t, &ContinueAction{}, actionList[0])
}

func TestGetActionList_NonMatchingRule(t *testing.T) {
	rule := rules.NewQueryRule("test_rule", "test_rule", rules.QRFail)
	rule.SetIPCond("1.1.1.1")
	qrs := rules.New()
	qrs.Add(rule)
	actionList := GetActionList(qrs, "", "", nil, sqlparser.MarginComments{})
	assert.Equal(t, 1, len(actionList))
	assert.IsType(t, &ContinueAction{}, actionList[0])
}

func TestCreateActionInstance(t *testing.T) {
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
				rule:   rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue),
			},
			want:    &ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
			wantErr: assert.NoError,
		},
		{
			name: "QRFail",
			args: args{
				action: rules.QRFail,
				rule:   rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail),
			},
			want:    &FailAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail), Action: rules.QRFail},
			wantErr: assert.NoError,
		},
		{
			name: "QRFailRetry",
			args: args{
				action: rules.QRFailRetry,
				rule:   rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFailRetry),
			},
			want:    &FailRetryAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFailRetry), Action: rules.QRFailRetry},
			wantErr: assert.NoError,
		},
		{
			name: "QRConcurrencyControl",
			args: args{
				action: rules.QRConcurrencyControl,
				rule:   rules.NewQueryRule("ruleDescription", "test_rule", rules.QRConcurrencyControl),
			},
			want:    &ConcurrencyControlAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRConcurrencyControl), Action: rules.QRConcurrencyControl},
			wantErr: assert.NoError,
		},
		{
			name: "unknown action",
			args: args{
				action: rules.Action(100),
				rule:   rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue),
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
			want: &ContinueAction{Rule: &rules.Rule{Name: "continue_action", Priority: DefaultPriority}, Action: rules.QRContinue},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, CreateContinueAction(), "CreateContinueAction()")
		})
	}
}

func Test_sortAction(t *testing.T) {
	r1 := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail)
	r2 := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail)
	r3 := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail)

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
