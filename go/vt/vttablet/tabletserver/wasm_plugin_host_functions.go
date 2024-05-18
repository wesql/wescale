package tabletserver

import (
	"context"
	"unsafe"

	"github.com/tetratelabs/wazero/api"
)

func SetGlobalValueByKeyHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroRuntime, keyPtr, keySize, valuePtr, valueSize uint32) uint32 {
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

	wazeroRuntime.globalHostVariables[key] = valueBytes
	return uint32(StatusOK)
}

func GetGlobalValueByKeyHost(ctx context.Context, mod api.Module, wazeroRuntime *WazeroRuntime, keyPtr, keySize, returnValuePtr, returnValueSize uint32) uint32 {
	wazeroRuntime.mu.Lock()
	defer wazeroRuntime.mu.Unlock()

	keyBytes, ok := mod.Memory().Read(keyPtr, keySize)
	if !ok {
		return uint32(StatusInternalFailure)
	}
	key := string(keyBytes)

	_, exist := wazeroRuntime.globalHostVariables[key]
	if !exist {
		return uint32(StatusNotFound)
	}

	hostDataPtr := &wazeroRuntime.globalHostVariables[key][0]
	hostDataSize := len(wazeroRuntime.globalHostVariables[key])
	return uint32(copyBytesToWasm(ctx, mod, hostDataPtr, hostDataSize, returnValuePtr, returnValueSize))
}

func SetModuleValueByKeyHost(hostModulePtr uint64, key, value uint32) {
	//m := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	//m.mu.Lock()
	//defer m.mu.Unlock()
	//
	//m.moduleHostVariables[strconv.Itoa(int(key))] = value
}

func GetModuleValueByKeyHost(hostModulePtr uint64, key uint32) uint32 {
	//m := (*WazeroModule)(unsafe.Pointer(uintptr(hostModulePtr)))
	//m.mu.Lock()
	//defer m.mu.Unlock()
	//
	//_, exist := m.moduleHostVariables[strconv.Itoa(int(key))]
	//if !exist {
	//	m.moduleHostVariables[strconv.Itoa(int(key))] = uint32(0)
	//}
	//return m.moduleHostVariables[strconv.Itoa(int(key))].(uint32)
	return 0
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
