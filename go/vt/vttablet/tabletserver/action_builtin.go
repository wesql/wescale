package tabletserver

import (
	"context"
	"time"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"
)

type ContinueAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *ContinueAction) BeforeExecution(qre *QueryExecutor) error {
	return nil
}

func (p *ContinueAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *ContinueAction) SetParams(stringParams string) error {
	return nil
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

func (p *FailAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *FailAction) SetParams(stringParams string) error {
	return nil
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

func (p *FailRetryAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *FailRetryAction) SetParams(stringParams string) error {
	return nil
}

func (p *FailRetryAction) GetRule() *rules.Rule {
	return p.Rule
}

type ConcurrencyControlAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action

	maxConcurrency int `json:"max_concurrency"`
}

func (p *ConcurrencyControlAction) BeforeExecution(qre *QueryExecutor) error {
	doneFunc, waited, err := qre.tsv.qe.txSerializer.Wait(qre.ctx, qre.plan.QueryTemplateID, "t1")

	if waited {
		qre.tsv.stats.WaitTimings.Record("TxSerializer", time.Now())
	}
	if err != nil {
		return err
	}
	qre.ctx = context.WithValue(qre.ctx, "txSerializerDoneFunc", doneFunc)

	return nil
}

func (p *ConcurrencyControlAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	doneFunc := qre.ctx.Value("txSerializerDoneFunc").(txserializer.DoneFunc)
	doneFunc()
	return &ActionExecutionResponse{
		FireNext: true,
		Reply:    reply,
		Err:      err,
	}
}

func (p *ConcurrencyControlAction) SetParams(stringParams string) error {
	return nil
}

func (p *ConcurrencyControlAction) GetRule() *rules.Rule {
	return p.Rule
}
