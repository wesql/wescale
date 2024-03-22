package plugin

import (
	"fmt"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

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
