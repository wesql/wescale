package tabletserver

import (
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

type ContinueAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *ContinueAction) BeforeExecution(qre *QueryExecutor) error {
	return nil
}

func (p *ContinueAction) AfterExecution(reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *ContinueAction) GetRule() *rules.Rule {
	return p.Rule
}

type FailAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailAction) BeforeExecution(qre *QueryExecutor) error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailAction) AfterExecution(reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *FailAction) GetRule() *rules.Rule {
	return p.Rule
}

type FailRetryAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *FailRetryAction) BeforeExecution(qre *QueryExecutor) error {
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailRetryAction) AfterExecution(reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *FailRetryAction) GetRule() *rules.Rule {
	return p.Rule
}

type ConcurrencyControlAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *ConcurrencyControlAction) BeforeExecution(qre *QueryExecutor) error {
	return nil
}

func (p *ConcurrencyControlAction) AfterExecution(reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *ConcurrencyControlAction) GetRule() *rules.Rule {
	return p.Rule
}
