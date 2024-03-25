package tabletserver

import (
	"fmt"
	"sort"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

// GetPluginList runs the input against the rules engine and returns the action list to be performed.
func GetPluginList(
	qrs *rules.Rules,
	ip,
	user string,
	bindVars map[string]*querypb.BindVariable,
	marginComments sqlparser.MarginComments,
) (action []PluginInterface) {
	var actionList []PluginInterface
	qrs.ForEachRule(func(qr *rules.Rule) {
		act := qr.FilterByExecutionInfo(ip, user, bindVars, marginComments)
		p, err := CreatePlugin(act, qr)
		if err != nil {
			actionList = append(actionList, CreateNoOpPlugin())
			return
		}
		actionList = append(actionList, p)
	})
	if len(actionList) == 0 {
		actionList = append(actionList, CreateNoOpPlugin())
	}
	sort.SliceStable(actionList, func(i, j int) bool {
		return actionList[i].GetRule().Priority < actionList[j].GetRule().Priority
	})
	return actionList
}

func CreatePlugin(action rules.Action, rule *rules.Rule) (PluginInterface, error) {
	switch action {
	case rules.QRContinue:
		return &NoOpPlugin{Rule: rule, Action: action}, nil
	case rules.QRFail:
		return &FailPlugin{Rule: rule, Action: action}, nil
	case rules.QRFailRetry:
		return &FailRetryPlugin{Rule: rule, Action: action}, nil
	default:
		log.Errorf("unknown action: %v", action)
		//todo earayu: maybe we should use 'vterrors.Errorf' here
		return nil, fmt.Errorf("unknown action: %v", action)
	}
}

func CreateNoOpPlugin() PluginInterface {
	return &NoOpPlugin{Rule: &rules.Rule{Name: "noop", Priority: DefaultPriority}, Action: rules.QRContinue}
}
