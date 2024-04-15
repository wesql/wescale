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
	var actionList = make([]ActionInterface, 0)
	qrs.ForEachRule(func(qr *rules.Rule) {
		if qr.Status == rules.InActive {
			log.Errorf("rule %s is inactive", qr.Name)
			return
		}
		act := qr.FilterByExecutionInfo(ip, user, bindVars, marginComments)
		if act == rules.QRContinue {
			return
		}
		p, err := CreateActionInstance(act, qr)
		if err != nil {
			return
		}
		actionList = append(actionList, p)
	})
	sortAction(actionList)
	for i, act := range actionList {
		if !act.GetRule().FireNext {
			actionList = actionList[:i+1]
			break
		}
	}
	return actionList
}

func sortAction(actionList []ActionInterface) {
	sort.SliceStable(actionList, func(i, j int) bool {
		return actionList[i].GetRule().Priority < actionList[j].GetRule().Priority
	})
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
		//todo filter: maybe we should use 'vterrors.Errorf' here
		actInst, err = nil, fmt.Errorf("unknown action: %v", action)
	}

	if actInst != nil {
		err = actInst.SetParams(rule.GetActionArgs())
	}
	return actInst, err
}

func CreateContinueAction() ActionInterface {
	return &ContinueAction{Rule: &rules.Rule{Name: "continue_action", Priority: DefaultPriority}, Action: rules.QRContinue}
}
