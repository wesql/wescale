package tabletserver

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

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
