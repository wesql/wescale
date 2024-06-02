package tabletserver

import (
	"context"
	"fmt"
	"regexp"
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
			"make sure MaxQueueSize == 0 || (MaxConcurrency > 0 && MaxConcurrency <= MaxQueueSize)", c.MaxQueueSize, c.MaxConcurrency)
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

// todo newborn22: add testcase
type WasmPluginActionArgs struct {
	WasmBinaryName string `toml:"wasm_binary_name"`
}

// todo newborn22: add testcase
func (args *WasmPluginActionArgs) Parse(stringParams string) (ActionArgs, error) {
	if stringParams == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "wasm bytes is empty")
	}

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
	controller := qre.tsv.qe.wasmPluginController
	wasmModuleCacheKey := p.GetRule().Name

	ok, module := controller.VM.GetWasmModule(wasmModuleCacheKey)
	if !ok {
		wasmBytes, err := controller.GetWasmBytesByBinaryName(qre.ctx, p.Args.WasmBinaryName)
		if err != nil {
			return &ActionExecutionResponse{Err: err}
		}
		module, err = controller.VM.InitWasmModule(wasmModuleCacheKey, wasmBytes)
		if err != nil {
			return &ActionExecutionResponse{Err: err}
		}
	}

	instance, err := module.NewInstance(qre)
	if err != nil {
		return &ActionExecutionResponse{Err: err}
	}

	qre.ctx = context.WithValue(qre.ctx, "wasmInstance", instance)

	err = instance.RunWASMPlugin()
	return &ActionExecutionResponse{Err: err}
}

func (p *WasmPluginAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	v := qre.ctx.Value("wasmInstance")
	if v == nil {
		return &ActionExecutionResponse{Reply: reply, Err: fmt.Errorf("fail to get wasm instance after query execution")}
	}
	instance := v.(WasmInstance)
	defer instance.Close()

	if err != nil {
		instance.SetErrorMessage(err.Error())
	}
	instance.SetQueryResult(reply)

	errFromGuest := instance.RunWASMPluginAfter()

	reply = instance.GetQueryResult()

	if reply == nil && errFromGuest == nil {
		return &ActionExecutionResponse{Reply: reply, Err: fmt.Errorf("unknown error in wasm plugin")}
	}
	return &ActionExecutionResponse{Reply: reply, Err: errFromGuest}
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

type SkipFilterAction struct {
	Rule *rules.Rule

	// Action is the action to take if the rule matches
	Action rules.Action

	Args *SkipFilterActionArgs
}

type SkipFilterActionArgs struct {
	AllowListRegexString string `toml:"allow_list"`
	AllowListRegex       *regexp.Regexp
}

func (args *SkipFilterActionArgs) Parse(stringParams string) (ActionArgs, error) {
	s := &SkipFilterActionArgs{}
	if stringParams == "" {
		// todo newborn22 6.2，让前端都变成*? 还是说这里也允许
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "stringParams is empty when parsing skip filter action args")
	}

	userInputTOML := ConvertUserInputToTOML(stringParams)
	err := toml.Unmarshal([]byte(userInputTOML), s)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error when parsing skip filter action args: %v", err)
	}
	if s.AllowListRegexString == "" {
		// todo newborn22 6.2，让前端都变成*? 还是说这里也允许
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "skip filter action args is empty")
	}
	s.AllowListRegex, err = regexp.Compile(fmt.Sprintf("^%s$", s.AllowListRegexString))
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error when compiling skip filter action args: %v", err)
	}

	return s, nil
}

func (s *SkipFilterAction) BeforeExecution(qre *QueryExecutor) *ActionExecutionResponse {
	// todo newborn22 6.2 do nothing here?
	var newActionList = make([]ActionInterface, 0)
	findSelf := false
	for _, a := range qre.matchedActionList {
		if a.GetRule().Name == s.GetRule().Name {
			findSelf = true
			continue
		}
		if findSelf {
			if s.Args.AllowListRegex.MatchString(a.GetRule().Name) {
				continue
			}
		}
		newActionList = append(newActionList, a)
	}
	qre.matchedActionList = newActionList
	return &ActionExecutionResponse{Err: nil}
}

func (s *SkipFilterAction) AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse {
	return &ActionExecutionResponse{Reply: reply, Err: err}
}

func (s *SkipFilterAction) ParseParams(argsStr string) (ActionArgs, error) {
	return s.Args.Parse(argsStr)
}

func (s *SkipFilterAction) SetParams(args ActionArgs) error {
	skipFilterArgs, ok := args.(*SkipFilterActionArgs)
	if !ok {
		return fmt.Errorf("args :%v is not a valid SkipFilterActionArgs)", args)
	}
	s.Args = skipFilterArgs
	return nil
}

func (s *SkipFilterAction) GetRule() *rules.Rule {
	return s.Rule
}
