package tabletserver

import (
	"context"
	"fmt"
	"sync"
	"unsafe"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"vitess.io/vitess/go/sqltypes"
)

type WazeroVM struct {
	mu      sync.Mutex
	ctx     context.Context
	runtime wazero.Runtime
	modules map[string]WasmModule

	globalMu sync.Mutex

	hostSharedVariables map[string][]byte
	hostSharedMu        sync.Mutex
}

func initWazeroVM() *WazeroVM {
	ctx := context.Background()
	w := &WazeroVM{
		ctx:                 ctx,
		modules:             make(map[string]WasmModule),
		hostSharedVariables: make(map[string][]byte),
	}
	w.InitRuntime()
	return w
}

func exportHostABIV1(ctx context.Context, wazeroRuntime *WazeroVM) error {
	_, err := wazeroRuntime.runtime.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithParameterNames("returnValuePtr", "returnValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, returnValueData, returnValueSize uint32) uint32 {
			return GetAbiVersionOnHost(ctx, mod, returnValueData, returnValueSize)
		}).
		Export("GetAbiVersionOnHost").
		NewFunctionBuilder().
		WithParameterNames("returnValuePtr", "returnValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, returnValueData, returnValueSize uint32) uint32 {
			return GetRuntimeTypeOnHost(ctx, mod, returnValueData, returnValueSize)
		}).
		Export("GetRuntimeTypeOnHost").
		NewFunctionBuilder().
		WithParameterNames("ptr", "size").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, ptr, size uint32) uint32 {
			return InfoLogOnHost(ctx, mod, ptr, size)
		}).
		Export("InfoLogOnHost").
		NewFunctionBuilder().
		WithParameterNames("ptr", "size").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, ptr, size uint32) uint32 {
			return ErrorLogOnHost(ctx, mod, ptr, size)
		}).
		Export("ErrorLogOnHost").
		NewFunctionBuilder().
		WithParameterNames("scope", "hostModulePtr", "keyPtr", "keySize", "valuePtr", "valueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, scope uint32, hostModulePtr uint64, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
			return SetValueByKeyOnHost(ctx, mod, wazeroRuntime, scope, hostModulePtr, keyPtr, keySize, valuePtr, valueSize)
		}).
		Export("SetValueByKeyOnHost").
		NewFunctionBuilder().
		WithParameterNames("scope", "hostModulePtr", "keyPtr", "keySize", "returnValuePtr", "returnValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, scope uint32, hostModulePtr uint64, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
			return GetValueByKeyOnHost(ctx, mod, wazeroRuntime, scope, hostModulePtr, keyPtr, keySize, returnValuePtr, returnValueSize)
		}).
		Export("GetValueByKeyOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "returnQueryValueData",
			"returnQueryValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnValueData, returnValueSize uint32) uint32 {
			return GetQueryOnHost(ctx, mod, hostInstancePtr, returnValueData, returnValueSize)
		}).
		Export("GetQueryOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "queryValuePtr",
			"queryValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryValuePtr, queryValueSize uint32) uint32 {
			return SetQueryOnHost(ctx, mod, hostInstancePtr, queryValuePtr, queryValueSize)
		}).
		Export("SetQueryOnHost").
		NewFunctionBuilder().
		WithParameterNames("scope", "hostModulePtr").
		WithFunc(func(ctx context.Context, mod api.Module, scope uint32, hostModulePtr uint64) {
			LockOnHost(wazeroRuntime, scope, hostModulePtr)
		}).
		Export("LockOnHost").
		NewFunctionBuilder().
		WithParameterNames("scope", "hostModulePtr").
		WithFunc(func(ctx context.Context, mod api.Module, scope uint32, hostModulePtr uint64) {
			UnlockOnHost(wazeroRuntime, scope, hostModulePtr)
		}).
		Export("UnlockOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "errMessagePtr", "errMessageSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, errMessagePtr, errMessageSize uint32) uint32 {
			return SetErrorMessageOnHost(ctx, mod, hostInstancePtr, errMessagePtr, errMessageSize)
		}).
		Export("SetErrorMessageOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "errMessagePtr", "errMessageSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, errMessagePtr, errMessageSize uint32) uint32 {
			return GetErrorMessageOnHost(ctx, mod, hostInstancePtr, errMessagePtr, errMessageSize)
		}).
		Export("GetErrorMessageOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "returnQueryResultPtr",
			"returnQueryResultSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnQueryResultPtr, returnQueryResultSize uint32) uint32 {
			return GetQueryResultOnHost(ctx, mod, hostInstancePtr, returnQueryResultPtr, returnQueryResultSize)
		}).
		Export("GetQueryResultOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "queryResultPtr",
			"queryResultSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryResultPtr, queryResultSize uint32) uint32 {
			return SetQueryResultOnHost(ctx, mod, hostInstancePtr, queryResultPtr, queryResultSize)
		}).
		Export("SetQueryResultOnHost").
		Instantiate(ctx)
	return err
}

func (*WazeroVM) GetRuntimeType() string {
	return WAZERO
}

func (w *WazeroVM) InitRuntime() error {
	w.runtime = wazero.NewRuntimeWithConfig(w.ctx, wazero.NewRuntimeConfig().WithCompilationCache(wazero.NewCompilationCache()))
	wasi_snapshot_preview1.MustInstantiate(w.ctx, w.runtime)
	return exportHostABIV1(w.ctx, w)
}

func (w *WazeroVM) ClearWasmModule(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if mod, exist := w.modules[key]; exist {
		defer mod.Close()
	}
	delete(w.modules, key)
}

func (w *WazeroVM) GetWasmModule(key string) (bool, WasmModule) {
	w.mu.Lock()
	defer w.mu.Unlock()
	module, exist := w.modules[key]
	return exist, module
}

func (w *WazeroVM) SetWasmModule(key string, wasmModule WasmModule) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// the goal is to change the compiled module bytes, and keep module variables map
	oldWasmModule, exist := w.modules[key]
	if exist {
		oldMap := oldWasmModule.GetModuleSharingVariables()
		wasmModule.SetModuleSharingVariables(oldMap)
	}
	w.modules[key] = wasmModule
}

func (w *WazeroVM) InitWasmModule(key string, wasmBytes []byte) (WasmModule, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	module, exist := w.modules[key]
	if exist {
		return module, nil
	}
	module, err := w.CompileWasmModule(wasmBytes)
	if err != nil {
		return nil, err
	}
	w.modules[key] = module
	return module, nil
}

func (w *WazeroVM) CompileWasmModule(wasmBytes []byte) (WasmModule, error) {
	module, err := w.runtime.CompileModule(w.ctx, wasmBytes)
	if err != nil {
		return nil, err
	}

	return &WazeroModule{compliedModule: module, wazeroRuntime: w, moduleSharingVariables: make(map[string][]byte)}, nil
}

func (w *WazeroVM) Close() error {
	w.runtime.Close(w.ctx)
	return nil
}

type WazeroModule struct {
	wazeroRuntime  *WazeroVM
	compliedModule wazero.CompiledModule

	mu                     sync.Mutex
	moduleSharingVariables map[string][]byte

	moduleMu sync.Mutex
	tmp      int
}

func (mod *WazeroModule) GetModuleSharingVariables() map[string][]byte {
	mod.mu.Lock()
	defer mod.mu.Unlock()
	return mod.moduleSharingVariables
}

func (mod *WazeroModule) SetModuleSharingVariables(m map[string][]byte) {
	mod.mu.Lock()
	defer mod.mu.Unlock()
	mod.moduleSharingVariables = m
}

func (mod *WazeroModule) NewInstance(qre *QueryExecutor) (WasmInstance, error) {
	if mod.wazeroRuntime == nil {
		return nil, fmt.Errorf("wazeroRuntime is nil in NewInstance")
	}
	if mod.compliedModule == nil {
		return nil, fmt.Errorf("compliedModule is nil in NewInstance")
	}
	instance, err := mod.wazeroRuntime.runtime.InstantiateModule(mod.wazeroRuntime.ctx, mod.compliedModule, wazero.NewModuleConfig().WithName(""))
	if err != nil {
		return nil, err
	}
	return &WazeroInstance{instance: instance, qre: qre, module: mod}, nil
}

func (mod *WazeroModule) Close() error {
	mod.compliedModule.Close(mod.wazeroRuntime.ctx)
	return nil
}

type WazeroInstance struct {
	instance api.Module
	qre      *QueryExecutor

	module       *WazeroModule
	errorMessage string

	queryResult *sqltypes.Result
}

func (ins *WazeroInstance) RunWASMPlugin() error {
	ctx := context.Background()

	wazeroGuestFunc := ins.instance.ExportedFunction("RunBeforeExecutionOnGuest")

	instancePtr := uint64(uintptr(unsafe.Pointer(ins)))
	modulePtr := uint64(uintptr(unsafe.Pointer(ins.module)))
	_, err := wazeroGuestFunc.Call(ctx, instancePtr, modulePtr)
	if err != nil {
		return err
	}
	if ins.errorMessage != "" {
		return fmt.Errorf("error from wasm plugin at before execution stage: %s", ins.errorMessage)
	}
	return nil
}

func (ins *WazeroInstance) RunWASMPluginAfter() error {
	ctx := context.Background()

	wazeroGuestFunc := ins.instance.ExportedFunction("RunAfterExecutionOnGuest")

	_, err := wazeroGuestFunc.Call(ctx)
	if err != nil {
		return err
	}
	if ins.errorMessage != "" {
		return fmt.Errorf("error from wasm plugin at after execution stage: %s", ins.errorMessage)
	}
	return nil
}

func (ins *WazeroInstance) SetErrorMessage(message string) {
	ins.errorMessage = message
}

func (ins *WazeroInstance) SetQueryResult(qr *sqltypes.Result) {
	ins.queryResult = qr
}

func (ins *WazeroInstance) GetQueryResult() *sqltypes.Result {
	return ins.queryResult
}

func (ins *WazeroInstance) Close() error {
	ins.instance.Close(ins.module.wazeroRuntime.ctx)
	return nil
}

func copyHostStringIntoGuest(ctx context.Context, mod api.Module, str string, wasmPtrPtr uint32, wasmSizePtr uint32) Status {
	bytes := []byte(str)
	return copyHostBytesIntoGuest(ctx, mod, bytes, wasmPtrPtr, wasmSizePtr)
}

func copyHostBytesIntoGuest(ctx context.Context, mod api.Module, bytes []byte, wasmPtrPtr uint32, wasmSizePtr uint32) Status {
	size := len(bytes)
	if size == 0 {
		return StatusBadArgument
	}

	//todo wasm: why not use 'malloc' here?
	alloc := mod.ExportedFunction("proxy_on_memory_allocate")
	res, err := alloc.Call(ctx, uint64(size))
	if err != nil {
		return StatusInternalFailure
	}
	buf, ok := mod.Memory().Read(uint32(res[0]), uint32(size))
	if !ok {
		return StatusInternalFailure
	}

	copy(buf, bytes)
	ok = mod.Memory().WriteUint32Le(wasmPtrPtr, uint32(res[0]))
	if !ok {
		return StatusInternalFailure
	}
	ok = mod.Memory().WriteUint32Le(wasmSizePtr, uint32(size))
	if !ok {
		return StatusInternalFailure
	}
	return StatusOK
}
