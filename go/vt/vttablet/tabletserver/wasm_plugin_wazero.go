package tabletserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
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

	hostVariables map[string]any
}

func initWazeroRuntime(qe *QueryEngine) *WazeroRuntime {
	ctx := context.Background()
	w := &WazeroRuntime{ctx: ctx, runtime: wazero.NewRuntime(ctx), modules: make(map[string]WasmModule), qe: qe, hostVariables: make(map[string]any)}

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
		// SetValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("key", "value").
		WithFunc(func(ctx context.Context, mod api.Module, key, value uint32) {
			wazeroRuntime.hostVariables[strconv.Itoa(int(key))] = value
		}).
		Export("SetValueByKeyHost").
		// GetValueByKeyHost
		NewFunctionBuilder().
		WithParameterNames("key").
		WithResultNames("value").
		WithFunc(func(ctx context.Context, mod api.Module, key uint32) uint32 {
			// todo
			_, exist := wazeroRuntime.hostVariables[strconv.Itoa(int(key))]
			if !exist {
				wazeroRuntime.hostVariables[strconv.Itoa(int(key))] = uint32(0)
			}
			return wazeroRuntime.hostVariables[strconv.Itoa(int(key))].(uint32)
		}).
		Export("GetValueByKeyHost").
		// GetQueryHost
		NewFunctionBuilder().
		WithParameterNames("return_query_value_data",
			"return_query_value_size").
		WithResultNames("call_result").
		WithFunc(func(ctx context.Context, mod api.Module, returnValueData,
			returnValueSize uint32) uint32 {
			var returnValueHostPtr *byte
			var returnValueSizePtr int
			ret := uint32(ProxyGetQuery(nil, &returnValueHostPtr, &returnValueSizePtr))
			copyBytesToWasm(ctx, mod, returnValueHostPtr, returnValueSizePtr, returnValueData, returnValueSize)
			return ret
		}).
		Export("GetQueryHost").
		// SetQueryHost
		NewFunctionBuilder().
		WithParameterNames("query_value_ptr",
			"query_value_size").
		WithResultNames("call_result").
		WithFunc(func(ctx context.Context, mod api.Module, queryValuePtr,
			queryValueSize uint32) uint32 {

			bytes, ok := mod.Memory().Read(queryValuePtr, queryValueSize)
			//todo newborn22, assign to query, perhaps every one has a qre
			if !ok {
				log.Printf("not ok!!!!!")
			} else {
				wazeroRuntime.hostVariables["haha"] = string(bytes)
				log.Printf("wasm setquery: %v", bytes)
			}

			return uint32(StatusOK)

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

func ProxyGetQuery(wazeroRuntime *WazeroRuntime, dataPtrPtr **byte, dataSizePtr *int) Status {
	// todo, how to set query?
	data := []byte("select * from foo.bar")
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

	return &WazeroModule{compliedModule: module, wazeroRuntime: w}, nil

}

func (w *WazeroRuntime) GetWasmInstance(key string, wasmBinaryName string) (WasmInstance, error) {
	module, err := w.InitOrGetWasmModule(key, wasmBinaryName)
	if err != nil {
		return nil, err
	}
	instance, err := module.NewInstance()
	if err != nil {
		return nil, err
	}
	return instance, nil
}

type WazeroModule struct {
	wazeroRuntime  *WazeroRuntime
	compliedModule wazero.CompiledModule
}

func (mod *WazeroModule) NewInstance() (WasmInstance, error) {
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
	return &WazeroInstance{instance: instance}, nil
}

type WazeroInstance struct {
	instance api.Module
}

func (ins *WazeroInstance) RunWASMPlugin(args *WasmPluginExchange) (*WasmPluginExchange, error) {
	ctx := context.Background()

	// todo use const or something else?
	wazeroGuestFunc := ins.instance.ExportedFunction("wazeroGuestFunc")
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

	ptrSize, err := wazeroGuestFunc.Call(ctx, dataPtr, uint64(len(dataStr)))
	if err != nil {
		return nil, err
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
	rst := &WasmPluginExchange{}
	err = json.Unmarshal(bytes, rst)
	return rst, err

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

func copyBytesToWasm(ctx context.Context, mod api.Module, hostPtr *byte, size int, wasmPtrPtr uint32, wasmSizePtr uint32) {
	if size == 0 {
		return
	}
	var hostSlice []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&hostSlice))
	hdr.Data = uintptr(unsafe.Pointer(hostPtr))
	hdr.Cap = size
	hdr.Len = size

	alloc := mod.ExportedFunction("proxy_on_memory_allocate")
	res, err := alloc.Call(ctx, uint64(size))
	if err != nil {
		log.Panicln(err)
	}
	buf, _ := mod.Memory().Read(uint32(res[0]), uint32(size))
	// todo newborn22 handle ok

	copy(buf, hostSlice)
	_ = mod.Memory().WriteUint32Le(wasmPtrPtr, uint32(res[0]))
	// todo newborn22 handle ok
	_ = mod.Memory().WriteUint32Le(wasmSizePtr, uint32(size))
	// todo newborn22 handle ok
}
