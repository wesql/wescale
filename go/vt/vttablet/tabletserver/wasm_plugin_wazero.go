package tabletserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type WazeroRuntime struct {
	mu        sync.Mutex
	ctx       context.Context
	runtime   wazero.Runtime
	instances map[string]WasmInstance
	qe        *QueryEngine
}

func initWazeroRuntime(qe *QueryEngine) *WazeroRuntime {
	ctx := context.Background()
	return &WazeroRuntime{ctx: ctx, runtime: wazero.NewRuntime(ctx), instances: make(map[string]WasmInstance), qe: qe}
}

func (*WazeroRuntime) GetRuntimeType() string {
	return WAZERO
}

func (w *WazeroRuntime) InitOrGetWasmInstance(key string, wasmBinaryName string) (WasmInstance, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	instance, exist := w.instances[key]
	if exist {
		return instance, nil
	}
	instance, err := w.initWasmInstance(wasmBinaryName)
	if err != nil {
		return nil, err
	}
	w.instances[key] = instance
	return instance, nil
}

func (w *WazeroRuntime) initWasmInstance(wasmBinaryName string) (WasmInstance, error) {
	wasmBytes, err := w.qe.GetWasmBytesByBinaryName(wasmBinaryName)
	if err != nil {
		return nil, err
	}

	// Compiles the module
	wasi_snapshot_preview1.MustInstantiate(w.ctx, w.runtime)

	module, err := w.runtime.Instantiate(w.ctx, wasmBytes)
	if err != nil {
		return nil, err
	}

	return &WazeroInstance{module: module}, nil

}

type WazeroInstance struct {
	module api.Module
}

func (ins *WazeroInstance) RunWASMPlugin(args *WasmPluginExchange) (*WasmPluginExchange, error) {
	ctx := context.Background()

	// todo use const or something else?
	wazeroGuestFunc := ins.module.ExportedFunction("wazeroGuestFunc")
	// These are undocumented, but exported. See tinygo-org/tinygo#2788
	malloc := ins.module.ExportedFunction("malloc")
	free := ins.module.ExportedFunction("free")

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
	if !ins.module.Memory().Write(uint32(dataPtr), []byte(dataStr)) {
		return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			dataPtr, uint64(len(dataStr)), ins.module.Memory().Size())
	}

	ptrSize, err := wazeroGuestFunc.Call(ctx, dataPtr, uint64(len(dataStr)))
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
	bytes, ok := ins.module.Memory().Read(rstFromGuestPtr, rstFromGuestSize)
	if !ok {
		return nil, fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
			dataPtr, uint64(len(dataStr)), ins.module.Memory().Size())
	}
	rst := &WasmPluginExchange{}
	err = json.Unmarshal(bytes, rst)
	return rst, err

}
