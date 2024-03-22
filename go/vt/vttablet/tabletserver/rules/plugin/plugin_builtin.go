package plugin

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

func (p *NoOpPlugin) BeforeExecution() error {
	return nil
}

func (p *NoOpPlugin) AfterExecution() error {
	return nil
}

func (p *NoOpPlugin) GetPriority() int {
	return defaultPriority
}

type FailPlugin struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailPlugin) BeforeExecution() error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailPlugin) AfterExecution() error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailPlugin) GetPriority() int {
	return defaultPriority
}

type FailRetryPlugin struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailRetryPlugin) BeforeExecution() error {
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailRetryPlugin) AfterExecution() error {
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailRetryPlugin) GetPriority() int {
	return defaultPriority
}
