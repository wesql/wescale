package tabletserver

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

type NoOpPlugin struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *NoOpPlugin) BeforeExecution(qre *QueryExecutor) error {
	return nil
}

func (p *NoOpPlugin) GetRule() *rules.Rule {
	return p.Rule
}

type FailPlugin struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailPlugin) BeforeExecution(qre *QueryExecutor) error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailPlugin) GetRule() *rules.Rule {
	return p.Rule
}

type FailRetryPlugin struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailRetryPlugin) BeforeExecution(qre *QueryExecutor) error {
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailRetryPlugin) GetRule() *rules.Rule {
	return p.Rule
}
