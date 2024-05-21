package tabletserver

import (
	"context"
	"github.com/tetratelabs/wazero/api"
	"unsafe"
)

//todo wasm: add a abstract layer, should not rely on 'wazero' and 'WazeroVM'.
//todo wasm: should not rely on tabletserver, should be a independent module. Now 'SetQueryHost' needs tabletserver.

func GetAbiVersionHost(ctx context.Context, mod api.Module, returnValuePtr uint32, returnValueSize uint32) uint32 {
	//todo
	return uint32(StatusOK)
}

func GetRuntimeType(ctx context.Context, mod api.Module, returnValuePtr uint32, returnValueSize uint32) uint32 {
	//todo
	return uint32(StatusOK)
}

func SetGlobalValueByKeyHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroVM, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
	wazeroRuntime.mu.Lock()
	defer wazeroRuntime.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	valueBytes, ok := mod.Memory().Read(valuePtr, valueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}

	wazeroRuntime.hostSharedVariables[key] = valueBytes
	return uint32(StatusOK)
}

func GetGlobalValueByKeyHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroVM, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
	wazeroRuntime.mu.Lock()
	defer wazeroRuntime.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	_, exist := wazeroRuntime.hostSharedVariables[key]
	if !exist {
		return uint32(StatusNotFound)
	}

	hostDataPtr := &wazeroRuntime.hostSharedVariables[key][0]
	hostDataSize := len(wazeroRuntime.hostSharedVariables[key])
	return uint32(copyBytesToWasm(ctx, mod, hostDataPtr, hostDataSize, returnValuePtr, returnValueSize))
}

func SetModuleValueByKeyHost(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.mu.Lock()
	defer module.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	valueBytes, ok := mod.Memory().Read(valuePtr, valueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}

	module.moduleHostVariables[key] = valueBytes
	return uint32(StatusOK)
}

func GetModuleValueByKeyHost(ctx context.Context, mod api.Module, hostModulePtr uint64, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.mu.Lock()
	defer module.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	_, exist := module.moduleHostVariables[key]
	if !exist {
		return uint32(StatusNotFound)
	}

	hostDataPtr := &module.moduleHostVariables[key][0]
	hostDataSize := len(module.moduleHostVariables[key])
	return uint32(copyBytesToWasm(ctx, mod, hostDataPtr, hostDataSize, returnValuePtr, returnValueSize))
}

func GetQueryHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, returnValueData,
	returnValueSize uint32) uint32 {
	var returnValueHostPtr *byte
	var returnValueSizePtr int
	ret := uint32(ProxyGetQuery(hostInstancePtr, &returnValueHostPtr, &returnValueSizePtr))
	if 0 != ret {
		return ret
	}
	ret = uint32(copyBytesToWasm(ctx, mod, returnValueHostPtr, returnValueSizePtr, returnValueData, returnValueSize))
	return ret
}

func SetQueryHost(ctx context.Context, mod api.Module, hostInstancePtr uint64, queryValuePtr, queryValueSize uint32) uint32 {
	bytes, ok := mod.Memory().Read(queryValuePtr, queryValueSize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	w := (*WazeroInstance)(unsafe.Pointer(uintptr(hostInstancePtr)))
	err := SetQueryToQre(w.qre, string(bytes))
	if err != nil {
		return uint32(StatusInternalFailure)
	}
	return uint32(StatusOK)
}

func GlobalLockHost(wazeroRuntime *WazeroVM) {
	wazeroRuntime.mu.Lock()
}

func GlobalUnLockHost(wazeroRuntime *WazeroVM) {
	wazeroRuntime.mu.Unlock()
}

func ModuleLockHost(hostModulePtr uint64) {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.moduleMu.Lock()
}

func ModuleUnLockHost(hostModulePtr uint64) {
	module := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	module.moduleMu.Unlock()
}
