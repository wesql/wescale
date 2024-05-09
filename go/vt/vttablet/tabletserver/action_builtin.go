package tabletserver

import (
	"context"
	"fmt"
	"time"

	"github.com/BurntSushi/toml"

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

func (p *ContinueAction) BeforeExecution(_ *QueryExecutor) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: nil,
		Err:   nil,
	}
}

func (p *ContinueAction) AfterExecution(_ *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *ContinueAction) ParseParams(_ string) (ActionArgs, error) {
	return nil, nil
}

func (p *ContinueAction) SetParams(_ ActionArgs) error {
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

func (p *FailAction) BeforeExecution(_ *QueryExecutor) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: nil,
		Err:   vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "disallowed due to rule: %s", p.Rule.Name),
	}
}

func (p *FailAction) AfterExecution(_ *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *FailAction) ParseParams(_ string) (ActionArgs, error) {
	return nil, nil
}

func (p *FailAction) SetParams(_ ActionArgs) error {
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

func (p *FailRetryAction) BeforeExecution(_ *QueryExecutor) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: nil,
		Err:   vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "disallowed due to rule: %s", p.Rule.Name),
	}
}

func (p *FailRetryAction) AfterExecution(_ *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *FailRetryAction) ParseParams(_ string) (ActionArgs, error) {
	return nil, nil
}

func (p *FailRetryAction) SetParams(_ ActionArgs) error {
	return nil
}

func (p *FailRetryAction) GetRule() *rules.Rule {
	return p.Rule
}

type BufferAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action
}

func (p *BufferAction) BeforeExecution(qre *QueryExecutor) *ActionExecutionResponse {
	bufferingTimeoutCtx, cancel := context.WithTimeout(qre.ctx, maxQueryBufferDuration)
	defer cancel()

	ruleCancelCtx := p.GetRule().GetCancelCtx()
	if ruleCancelCtx != nil {
		// We buffer up to some timeout. The timeout is determined by ctx.Done().
		// If we're not at timeout yet, we fail the query
		select {
		case <-ruleCancelCtx.Done():
			// good! We have buffered the query, and buffering is completed
		case <-bufferingTimeoutCtx.Done():
			// Sorry, timeout while waiting for buffering to complete
			return &ActionExecutionResponse{
				Reply: nil,
				Err:   vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "buffer timeout in rule: %s", p.GetRule().Name),
			}
		}
	}

	return &ActionExecutionResponse{
		Reply: nil,
		Err:   nil,
	}
}

func (p *BufferAction) AfterExecution(_ *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *BufferAction) ParseParams(_ string) (ActionArgs, error) {
	return nil, nil
}

func (p *BufferAction) SetParams(_ ActionArgs) error {
	return nil
}

func (p *BufferAction) GetRule() *rules.Rule {
	return p.Rule
}

type ConcurrencyControlAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action

	Args *ConcurrencyControlActionArgs
}

type ConcurrencyControlActionArgs struct {
	MaxQueueSize   int `toml:"max_queue_size"`
	MaxConcurrency int `toml:"max_concurrency"`
}

func (args *ConcurrencyControlActionArgs) Parse(stringParams string) (ActionArgs, error) {
	if stringParams == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "action args is empty")
	}

	userInputTOML := ConvertUserInputToTOML(stringParams)

	c := &ConcurrencyControlActionArgs{}
	err := toml.Unmarshal([]byte(userInputTOML), c)
	if err != nil {
		return nil, fmt.Errorf("error when parsing action args: %v", err)
	}
	if !(c.MaxQueueSize == 0 || (c.MaxConcurrency > 0 && c.MaxConcurrency <= c.MaxQueueSize)) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "MaxQueueSize: %d, MaxConcurrency: %d, param value is invalid: "+
			"make sure MaxQueueSize == 0 || (MaxConcurrency > 0 && MaxConcurrency <= MaxQueueSize)", c.MaxQueueSize, c.MaxQueueSize)
	}
	return c, nil
}

func (p *ConcurrencyControlAction) BeforeExecution(qre *QueryExecutor) *ActionExecutionResponse {
	q := qre.tsv.qe.concurrencyController.GetOrCreateQueue(p.GetRule().Name, p.Args.MaxQueueSize, p.Args.MaxConcurrency)
	doneFunc, waited, err := q.Wait(qre.ctx, qre.plan.TableNames())

	if waited {
		qre.tsv.stats.WaitTimings.Record("ccl", time.Now())
	}
	if err != nil {
		return &ActionExecutionResponse{
			Reply: nil,
			Err:   err,
		}
	}
	qre.ctx = context.WithValue(qre.ctx, "cclDoneFunc", doneFunc)

	return &ActionExecutionResponse{
		Reply: nil,
		Err:   nil,
	}
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

func (p *ConcurrencyControlAction) ParseParams(stringParams string) (ActionArgs, error) {
	return p.Args.Parse(stringParams)
}

func (p *ConcurrencyControlAction) SetParams(args ActionArgs) error {
	cclArgs, ok := args.(*ConcurrencyControlActionArgs)
	if !ok {
		return fmt.Errorf("args :%v is not a valid ConcurrencyControlActionArgs)", args)
	}
	p.Args = cclArgs
	return nil
}

func (p *ConcurrencyControlAction) GetRule() *rules.Rule {
	return p.Rule
}

type WasmPluginAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action

	Args *WasmPluginActionArgs
}

type WasmPluginActionArgs struct {
	WasmBinaryName string `toml:"wasm_binary_name"`
}

func (args *WasmPluginActionArgs) Parse(stringParams string) (ActionArgs, error) {
	if stringParams == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "wasm bytes is empty")
	}

	// todo, check is in table?
	userInputTOML := ConvertUserInputToTOML(stringParams)
	w := &WasmPluginActionArgs{}
	err := toml.Unmarshal([]byte(userInputTOML), w)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error when parsing wasm plugin action args: %v", err)
	}
	if w.WasmBinaryName == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "wasm binary name is empty")
	}

	// the wasm bytes is valid or not will be checked when compiling it
	return w, nil
}

func (p *WasmPluginAction) BeforeExecution(qre *QueryExecutor) *ActionExecutionResponse {
	// todo by newborn22
	runtime := qre.tsv.qe.wasmPluginController.Runtime
	instance, err := runtime.InitOrGetWasmInstance(p.GetRule().Name, p.Args.WasmBinaryName)
	if err != nil {
		return &ActionExecutionResponse{
			Reply: nil,
			Err:   err,
		}
	}
	args := ConvertQueryExecutorToWasmPluginExchange(qre)
	rst, err := instance.RunWASMPlugin(args)
	if err != nil {
		return &ActionExecutionResponse{
			Reply: nil,
			Err:   err,
		}
	}
	ConvertWasmPluginExchangeToQueryExecutor(qre, rst)
	return &ActionExecutionResponse{
		Reply: nil,
		Err:   nil,
	}
}

func (p *WasmPluginAction) AfterExecution(_ *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{
		Reply: reply,
		Err:   err,
	}
}

func (p *WasmPluginAction) ParseParams(argsStr string) (ActionArgs, error) {
	return p.Args.Parse(argsStr)
}

func (p *WasmPluginAction) SetParams(args ActionArgs) error {
	wasmArgs, ok := args.(*WasmPluginActionArgs)
	if !ok {
		return fmt.Errorf("args :%v is not a valid WasmPluginAction)", args)
	}
	p.Args = wasmArgs
	return nil
}

func (p *WasmPluginAction) GetRule() *rules.Rule {
	return p.Rule
}
