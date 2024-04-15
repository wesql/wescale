package tabletserver

import (
	"context"
	"encoding/json"
	"time"

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/ccl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

type ContinueAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *ContinueAction) BeforeExecution(qre *QueryExecutor) (*sqltypes.Result, error) {
	return nil, nil
}

func (p *ContinueAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
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

func (p *FailAction) BeforeExecution(qre *QueryExecutor) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
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

func (p *FailRetryAction) BeforeExecution(qre *QueryExecutor) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Description)
}

func (p *FailRetryAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
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

	MaxQueueSize   int `json:"max_queue_size"`
	MaxConcurrency int `json:"max_concurrency"`
}

func (p *ConcurrencyControlAction) BeforeExecution(qre *QueryExecutor) (*sqltypes.Result, error) {
	q := qre.tsv.qe.concurrencyController.GetOrCreateQueue(qre.plan.QueryTemplateID, p.MaxQueueSize, p.MaxConcurrency)
	doneFunc, waited, err := q.Wait(qre.ctx, qre.plan.TableNames())

	if waited {
		qre.tsv.stats.WaitTimings.Record("ccl", time.Now())
	}
	if err != nil {
		return nil, err
	}
	qre.ctx = context.WithValue(qre.ctx, "cclDoneFunc", doneFunc)

	return nil, nil
}

func (p *ConcurrencyControlAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	v := qre.ctx.Value("cclDoneFunc")
	if v != nil {
		doneFunc := v.(ccl.DoneFunc)
		doneFunc()
	}

	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *ConcurrencyControlAction) SetParams(stringParams string) error {
	if stringParams == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "stringParams: %s is invalid", stringParams)
	}
	c := &ConcurrencyControlAction{}
	err := json.Unmarshal([]byte(stringParams), c)
	if err != nil {
		return err
	}
	if !(c.MaxQueueSize == 0 || (c.MaxConcurrency > 0 && c.MaxConcurrency <= c.MaxQueueSize)) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "MaxQueueSize: %d, MaxConcurrency: %d, param value is invalid: "+
			"make sure MaxQueueSize == 0 || (MaxConcurrency > 0 && MaxConcurrency <= MaxQueueSize)", c.MaxQueueSize, c.MaxQueueSize)
	}

	p.MaxQueueSize = c.MaxQueueSize
	p.MaxConcurrency = c.MaxConcurrency
	return nil
}

func (p *ConcurrencyControlAction) GetRule() *rules.Rule {
	return p.Rule
}
