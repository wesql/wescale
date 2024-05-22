package tabletserver

import (
	"context"
	"fmt"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"sync"
	"unsafe"
)

type WazeroVM struct {
	mu      sync.Mutex
	ctx     context.Context
	runtime wazero.Runtime
	modules map[string]WasmModule

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
		WithParameterNames("keyPtr", "keySize", "valuePtr", "valueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
			return SetGlobalValueByKeyOnHost(ctx, mod, wazeroRuntime, keyPtr, keySize, valuePtr, valueSize)
		}).
		Export("SetGlobalValueByKeyOnHost").
		NewFunctionBuilder().
		WithParameterNames("keyPtr", "keySize", "returnValuePtr", "returnValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
			return GetGlobalValueByKeyOnHost(ctx, mod, wazeroRuntime, keyPtr, keySize, returnValuePtr, returnValueSize)
		}).
		Export("GetGlobalValueByKeyOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr", "keyPtr", "keySize", "valuePtr", "valueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
			return SetModuleValueByKeyOnHost(ctx, mod, hostModulePtr, keyPtr, keySize, valuePtr, valueSize)
		}).
		Export("SetModuleValueByKeyOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr", "keyPtr", "keySize", "returnValuePtr", "returnValueSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
			return GetModuleValueByKeyOnHost(ctx, mod, hostModulePtr, keyPtr, keySize, returnValuePtr, returnValueSize)
		}).
		Export("GetModuleValueByKeyOnHost").
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
		WithFunc(func(ctx context.Context, mod api.Module) {
			GlobalLockOnHost(wazeroRuntime)
		}).
		Export("GlobalLockOnHost").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module) {
			GlobalUnLockOnHost(wazeroRuntime)
		}).
		Export("GlobalUnlockOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64) {
			ModuleLockOnHost(hostModulePtr)
		}).
		Export("ModuleLockOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64) {
			ModuleUnLockOnHost(hostModulePtr)
		}).
		Export("ModuleUnLockOnHost").
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "errMessagePtr", "errMessageSize").
		WithResultNames("callStatus").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, errMessagePtr, errMessageSize uint32) uint32 {
			return SetErrorMessageOnHost(ctx, mod, hostInstancePtr, errMessagePtr, errMessageSize)
		}).
		Export("SetErrorMessageOnHost").
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

func (w *WazeroVM) InitWasmModule(key string, wasmBytes []byte) (WasmModule, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	module, exist := w.modules[key]
	if exist {
		return module, nil
	}
	module, err := w.initWasmModule(wasmBytes)
	if err != nil {
		return nil, err
	}
	w.modules[key] = module
	return module, nil
}

func (w *WazeroVM) initWasmModule(wasmBytes []byte) (WasmModule, error) {
	module, err := w.runtime.CompileModule(w.ctx, wasmBytes)
	if err != nil {
		return nil, err
	}

	return &WazeroModule{compliedModule: module, wazeroRuntime: w, moduleHostVariables: make(map[string][]byte)}, nil
}

func (w *WazeroVM) Close() error {
	w.runtime.Close(w.ctx)
	return nil
}

type WazeroModule struct {
	wazeroRuntime  *WazeroVM
	compliedModule wazero.CompiledModule

	mu                  sync.Mutex
	moduleHostVariables map[string][]byte

	moduleMu sync.Mutex
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

	module              *WazeroModule
	errMessageFromGuest string
}

func (ins *WazeroInstance) RunWASMPlugin() error {
	ctx := context.Background()

	wazeroGuestFunc := ins.instance.ExportedFunction("WazeroGuestFuncBeforeExecution")

	instancePtr := uint64(uintptr(unsafe.Pointer(ins)))
	modulePtr := uint64(uintptr(unsafe.Pointer(ins.module)))
	_, err := wazeroGuestFunc.Call(ctx, instancePtr, modulePtr)
	if err != nil {
		return err
	}
	if ins.errMessageFromGuest != "" {
		return fmt.Errorf("error from wasm plugin: %s", ins.errMessageFromGuest)
	}
	return nil
}

func (ins *WazeroInstance) RunWASMPluginAfter(args *WasmPluginExchangeAfter) (*WasmPluginExchangeAfter, error) {
	//ctx := context.Background()
	//
	//// todo use const or something else?
	//wazeroGuestFuncAfterExecution := ins.instance.ExportedFunction("wazeroGuestFuncAfterExecution")
	//// These are undocumented, but exported. See tinygo-org/tinygo#2788
	//malloc := ins.instance.ExportedFunction("malloc")
	////free := ins.instance.ExportedFunction("free")
	//
	//data, err := json.Marshal(args)
	//if err != nil {
	//	return nil, err
	//}
	//dataStr := string(data)
	//dataLen := uint64(len(dataStr))
	//
	//results, err := malloc.Call(ctx, dataLen)
	//if err != nil {
	//	return nil, err
	//}
	//dataPtr := results[0]
	//// This pointer is managed by TinyGo, but TinyGo is unaware of external usage.
	//// So, we have to free it when finished
	////defer free.Call(ctx, dataPtr)
	//
	//// The pointer is a linear memory offset, which is where we write the data.
	//if !ins.instance.Memory().Write(uint32(dataPtr), []byte(dataStr)) {
	//	return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
	//		dataPtr, uint64(len(dataStr)), ins.instance.Memory().Size())
	//}
	//
	//ptrSize, err := wazeroGuestFuncAfterExecution.Call(ctx, dataPtr, uint64(len(dataStr)))
	//if err != nil {
	//	log.Panicln(err)
	//}
	//
	//rstFromGuestPtr := uint32(ptrSize[0] >> 32)
	//rstFromGuestSize := uint32(ptrSize[0])
	//
	//// This pointer is managed by TinyGo, but TinyGo is unaware of external usage.
	//// So, we have to free it when finished
	////if rstFromGuestPtr != 0 {
	////	defer func() {
	////		_, err := free.Call(ctx, uint64(rstFromGuestPtr))
	////		if err != nil {
	////			fmt.Print(err.Error())
	////		}
	////	}()
	////}
	//
	//// The pointer is a linear memory offset, which is where we write the name.
	//bytes, ok := ins.instance.Memory().Read(rstFromGuestPtr, rstFromGuestSize)
	//if !ok {
	//	return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
	//		dataPtr, uint64(len(dataStr)), ins.instance.Memory().Size())
	//}
	//rst := &WasmPluginExchangeAfter{}
	//err = json.Unmarshal(bytes, rst)
	//return rst, err
	return nil, nil
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
