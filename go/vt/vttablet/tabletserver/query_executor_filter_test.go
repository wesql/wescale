package tabletserver

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func TestQueryExecutor_runActionListBeforeExecution(t *testing.T) {

	tests := []struct {
		name       string
		actionList []ActionInterface
		wantErr    assert.ErrorAssertionFunc
	}{
		{
			name:       "nil action list",
			actionList: nil,
			wantErr:    assert.NoError,
		},
		{
			name:       "empty action list",
			actionList: make([]ActionInterface, 0),
			wantErr:    assert.NoError,
		},
		{
			name:       "action list with one QRContinue action",
			actionList: []ActionInterface{&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue}},
			wantErr:    assert.NoError,
		},
		{
			name:       "action list with one QRFail action",
			actionList: []ActionInterface{&FailAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail), Action: rules.QRFail}},
			wantErr:    assert.Error,
		},
		{
			name:       "action list with one QRFailRetry action",
			actionList: []ActionInterface{&FailRetryAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFailRetry), Action: rules.QRFailRetry}},
			wantErr:    assert.Error,
		},
		{
			name: "QRContinue, QRContinue, QRContinue",
			actionList: []ActionInterface{
				&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
			},
			wantErr: assert.NoError,
		},
		{
			name: "QRContinue, QRContinue, QRFail",
			actionList: []ActionInterface{
				&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRContinue), Action: rules.QRContinue},
				&FailAction{Rule: rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail), Action: rules.QRFail},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			qre := &QueryExecutor{ctx: ctx}
			qre.actionList = tt.actionList
			tt.wantErr(t, qre.runActionListBeforeExecution(), fmt.Sprintf("runActionListBeforeExecution()"))
		})
	}
}
