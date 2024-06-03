package tabletserver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func TestSkipFilter(t *testing.T) {

	skipRule := rules.NewActiveQueryRule("skip all filters", "s1", rules.QRSkipFilter)
	skipActionArgs := &SkipFilterActionArgs{}

	args, _ := skipActionArgs.Parse(`allow_regex=".*"`)
	skipActionArgs = args.(*SkipFilterActionArgs)
	skipFilter := &SkipFilterAction{Rule: skipRule, Action: rules.QRSkipFilter, Args: skipActionArgs}

	argsTestRegex, _ := skipActionArgs.Parse(`allow_regex="c.*|d1"`)
	skipActionArgsTestRegex := argsTestRegex.(*SkipFilterActionArgs)
	skipFilterTestRegex := &SkipFilterAction{Rule: skipRule, Action: rules.QRSkipFilter, Args: skipActionArgsTestRegex}

	tests := []struct {
		name                   string
		actionList             []ActionInterface
		expectedCalledNameList []string
	}{

		{
			name: "skip all",
			actionList: []ActionInterface{
				skipFilter,
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c1", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c2", rules.QRContinue), Action: rules.QRContinue}},
			expectedCalledNameList: []string{"s1"},
		},
		{
			name: "none to skip",
			actionList: []ActionInterface{
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c1", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c2", rules.QRContinue), Action: rules.QRContinue},
				skipFilter},
			expectedCalledNameList: []string{"c1", "c2", "s1"},
		},
		{
			name: "just skip c2",
			actionList: []ActionInterface{
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c1", rules.QRContinue), Action: rules.QRContinue},
				skipFilter,
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c2", rules.QRContinue), Action: rules.QRContinue}},
			expectedCalledNameList: []string{"c1", "s1"},
		},
		{
			name: "test regex",
			actionList: []ActionInterface{
				skipFilterTestRegex,
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c1", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "c2", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "d1", rules.QRContinue), Action: rules.QRContinue},
				&ContinueAction{Rule: rules.NewActiveQueryRule("", "d2", rules.QRContinue), Action: rules.QRContinue}},
			expectedCalledNameList: []string{"s1", "d2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			qre := &QueryExecutor{}
			qre.matchedActionList = tt.actionList
			_, err := qre.runActionListBeforeExecution()
			assert.Nil(t, err)
			assert.Equal(t, len(tt.expectedCalledNameList), len(qre.calledActionList))
			for i, name := range tt.expectedCalledNameList {
				assert.Equal(t, name, qre.calledActionList[i].GetRule().Name)
			}
		})
	}
}
