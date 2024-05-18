package tabletserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"unsafe"

	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

type WazeroRuntime struct {
	mu      sync.Mutex
	ctx     context.Context
	runtime wazero.Runtime
	modules map[string]WasmModule
	qe      *QueryEngine

	globalHostVariables map[string][]byte
}

func initWazeroRuntime(qe *QueryEngine) *WazeroRuntime {
	ctx := context.Background()
	w := &WazeroRuntime{ctx: ctx, runtime: wazero.NewRuntime(ctx), modules: make(map[string]WasmModule), qe: qe, globalHostVariables: make(map[string][]byte)}

	wasi_snapshot_preview1.MustInstantiate(w.ctx, w.runtime)
	err := exportHostABI(w.ctx, w)
	if err != nil {
		log.Fatal(err)
	}
	return w
}

// todo by newborn22
func exportHostABI(ctx context.Context, wazeroRuntime *WazeroRuntime) error {
	_, err := wazeroRuntime.runtime.NewHostModuleBuilder("env").
		// SetGlobalValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("keyPtr", "keySize", "valuePtr", "valueSize").
		WithResultNames("callResult").
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
			return SetGlobalValueByKeyHost(ctx, mod, wazeroRuntime, keyPtr, keySize, valuePtr, valueSize)
		}).
		Export("SetGlobalValueByKeyHost").

		// GetGlobalValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("keyPtr", "keySize", "returnValuePtr", "returnValueSize").
		WithResultNames("callResult").
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
			return GetGlobalValueByKeyHost(ctx, mod, wazeroRuntime, keyPtr, keySize, returnValuePtr, returnValueSize)
		}).
		Export("GetGlobalValueByKeyHost").

		// SetModuleValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr", "key", "value").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64, key, value uint32) {
			SetModuleValueByKeyHost(hostModulePtr, key, value)
		}).
		Export("SetModuleValueByKeyHost").

		// GetModuleValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("hostModulePtr", "key").
		WithResultNames("value").
		WithFunc(func(ctx context.Context, mod api.Module, hostModulePtr uint64, key uint32) uint32 {
			return GetModuleValueByKeyHost(hostModulePtr, key)
		}).
		Export("GetModuleValueByKeyHost").

		// GetQueryHost
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "returnQueryValueData",
			"returnQueryValueSize").
		WithResultNames("callResult").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnValueData, returnValueSize uint32) uint32 {
			return GetQueryHost(ctx, mod, hostInstancePtr, returnValueData, returnValueSize)
		}).
		Export("GetQueryHost").

		// SetQueryHost
		NewFunctionBuilder().
		WithParameterNames("hostInstancePtr", "queryValuePtr",
			"queryValueSize").
		WithResultNames("callResult").
		WithFunc(func(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryValuePtr, queryValueSize uint32) uint32 {
			return SetQueryHost(ctx, mod, hostInstancePtr, queryValuePtr, queryValueSize)
		}).
		Export("SetQueryHost").
		Instantiate(ctx)
	return err
}

type Status uint32

const (
	StatusOK              Status = 0
	StatusNotFound        Status = 1
	StatusBadArgument     Status = 2
	StatusEmpty           Status = 7
	StatusCasMismatch     Status = 8
	StatusInternalFailure Status = 10
	StatusUnimplemented   Status = 12
)

func ProxyGetQuery(hostInstancePtr uint64, dataPtrPtr **byte, dataSizePtr *int) Status {
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	data := []byte(w.qre.query)
	*dataPtrPtr = &data[0]
	dataSize := len(data)
	*dataSizePtr = dataSize
	return StatusOK
}

func (*WazeroRuntime) GetRuntimeType() string {
	return WAZERO
}

func (w *WazeroRuntime) ClearWasmModule(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.modules, key)

}

func (w *WazeroRuntime) InitOrGetWasmModule(key string, wasmBinaryName string) (WasmModule, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	module, exist := w.modules[key]
	if exist {
		return module, nil
	}
	module, err := w.initWasmModule(wasmBinaryName)
	if err != nil {
		return nil, err
	}
	w.modules[key] = module
	return module, nil
}

func (w *WazeroRuntime) initWasmModule(wasmBinaryName string) (WasmModule, error) {
	wasmBytes, err := w.qe.GetWasmBytesByBinaryName(wasmBinaryName)
	if err != nil {
		return nil, err
	}
	module, err := w.runtime.CompileModule(w.ctx, wasmBytes)
	if err != nil {
		return nil, err
	}

	return &WazeroModule{compliedModule: module, wazeroRuntime: w, moduleHostVariables: make(map[string][]byte)}, nil

}

func (w *WazeroRuntime) GetWasmInstance(key string, wasmBinaryName string, qre *QueryExecutor) (WasmInstance, error) {
	module, err := w.InitOrGetWasmModule(key, wasmBinaryName)
	if err != nil {
		return nil, err
	}
	instance, err := module.NewInstance(qre)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

type WazeroModule struct {
	wazeroRuntime  *WazeroRuntime
	compliedModule wazero.CompiledModule

	mu                  sync.Mutex
	moduleHostVariables map[string][]byte
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

type WazeroInstance struct {
	instance api.Module
	qre      *QueryExecutor

	module *WazeroModule
}

func (ins *WazeroInstance) RunWASMPlugin() error {
	ctx := context.Background()

	wazeroGuestFunc := ins.instance.ExportedFunction("WazeroGuestFuncBeforeExecution")

	instancePtr := uint64(uintptr(unsafe.Pointer(ins)))
	modulePtr := uint64(uintptr(unsafe.Pointer(ins.module)))
	_, err := wazeroGuestFunc.Call(ctx, instancePtr, modulePtr)
	return err
}

func (ins *WazeroInstance) RunWASMPluginAfter(args *WasmPluginExchangeAfter) (*WasmPluginExchangeAfter, error) {
	ctx := context.Background()

	// todo use const or something else?
	wazeroGuestFuncAfterExecution := ins.instance.ExportedFunction("wazeroGuestFuncAfterExecution")
	// These are undocumented, but exported. See tinygo-org/tinygo#2788
	malloc := ins.instance.ExportedFunction("malloc")
	free := ins.instance.ExportedFunction("free")

	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	dataStr := string(data)
	dataLen := uint64(len(dataStr))

	results, err := malloc.Call(ctx, dataLen)
	if err != nil {
		return nil, err
	}
	dataPtr := results[0]
	// This pointer is managed by TinyGo, but TinyGo is unaware of external usage.
	// So, we have to free it when finished
	defer free.Call(ctx, dataPtr)

	// The pointer is a linear memory offset, which is where we write the data.
	if !ins.instance.Memory().Write(uint32(dataPtr), []byte(dataStr)) {
		return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			dataPtr, uint64(len(dataStr)), ins.instance.Memory().Size())
	}

	ptrSize, err := wazeroGuestFuncAfterExecution.Call(ctx, dataPtr, uint64(len(dataStr)))
	if err != nil {
		log.Panicln(err)
	}

	rstFromGuestPtr := uint32(ptrSize[0] >> 32)
	rstFromGuestSize := uint32(ptrSize[0])

	// This pointer is managed by TinyGo, but TinyGo is unaware of external usage.
	// So, we have to free it when finished
	if rstFromGuestPtr != 0 {
		defer func() {
			_, err := free.Call(ctx, uint64(rstFromGuestPtr))
			if err != nil {
				fmt.Print(err.Error())
			}
		}()
	}

	// The pointer is a linear memory offset, which is where we write the name.
	bytes, ok := ins.instance.Memory().Read(rstFromGuestPtr, rstFromGuestSize)
	if !ok {
		return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			dataPtr, uint64(len(dataStr)), ins.instance.Memory().Size())
	}
	rst := &WasmPluginExchangeAfter{}
	err = json.Unmarshal(bytes, rst)
	return rst, err

}

func copyBytesToWasm(ctx context.Context, mod api.Module, hostPtr *byte, size int, wasmPtrPtr uint32, wasmSizePtr uint32) Status {
	if size == 0 {
		return StatusBadArgument
	}
	var hostSlice []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&hostSlice))
	hdr.Data = uintptr(unsafe.Pointer(hostPtr))
	hdr.Cap = size
	hdr.Len = size

	alloc := mod.ExportedFunction("proxy_on_memory_allocate")
	res, err := alloc.Call(ctx, uint64(size))
	if err != nil {
		return StatusInternalFailure
	}
	buf, ok := mod.Memory().Read(uint32(res[0]), uint32(size))
	if !ok {
		return StatusInternalFailure
	}

	copy(buf, hostSlice)
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
