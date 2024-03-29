package tabletserver

import (
	"fmt"
	"sort"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

// GetActionList runs the input against the rules engine and returns the action list to be performed.
func GetActionList(
	qrs *rules.Rules,
	ip,
	user string,
	bindVars map[string]*querypb.BindVariable,
	marginComments sqlparser.MarginComments,
) (action []ActionInterface) {
	var actionList []ActionInterface
	qrs.ForEachRule(func(qr *rules.Rule) {
		act := qr.FilterByExecutionInfo(ip, user, bindVars, marginComments)
		p, err := CreateActionInstance(act, qr)
		if err != nil {
			actionList = append(actionList, CreateContinueAction())
			return
		}
		actionList = append(actionList, p)
	})
	if len(actionList) == 0 {
		actionList = append(actionList, CreateContinueAction())
	}
	sort.SliceStable(actionList, func(i, j int) bool {
		return actionList[i].GetRule().Priority < actionList[j].GetRule().Priority
	})
	return actionList
}

func CreateActionInstance(action rules.Action, rule *rules.Rule) (ActionInterface, error) {
	var actInst ActionInterface
	var err error
	switch action {
	case rules.QRContinue:
		actInst, err = &ContinueAction{Rule: rule, Action: action}, nil
	case rules.QRFail:
		actInst, err = &FailAction{Rule: rule, Action: action}, nil
	case rules.QRFailRetry:
		actInst, err = &FailRetryAction{Rule: rule, Action: action}, nil
	case rules.QRConcurrencyControl:
		actInst, err = &ConcurrencyControlAction{Rule: rule, Action: action}, nil
	default:
		log.Errorf("unknown action: %v", action)
		//todo earayu: maybe we should use 'vterrors.Errorf' here
		actInst, err = nil, fmt.Errorf("unknown action: %v", action)
	}

	actInst.SetParams(rule.GetActionArgs())
	return actInst, err
}

func CreateContinueAction() ActionInterface {
	return &ContinueAction{Rule: &rules.Rule{Name: "noop", Priority: DefaultPriority}, Action: rules.QRContinue}
}