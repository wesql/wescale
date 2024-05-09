package tabletserver

import (
	"encoding/binary"
	"encoding/json"
	"sync"

	"github.com/wasmerio/wasmer-go/wasmer"
)

func initWasmerRuntime(qe *QueryEngine) *WasmerRuntime {
	engine := wasmer.NewEngine()
	store := wasmer.NewStore(engine)
	return &WasmerRuntime{store: store, instances: make(map[string]WasmInstance), qe: qe}
}

type WasmerRuntime struct {
	mu        sync.Mutex
	store     *wasmer.Store
	instances map[string]WasmInstance
	qe        *QueryEngine
}

func (*WasmerRuntime) GetRuntimeType() string {
	return WASMER
}

func (w *WasmerRuntime) InitOrGetWasmInstance(key string, wasmBinaryName string) (WasmInstance, error) {
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

func (w *WasmerRuntime) initWasmInstance(wasmBinaryName string) (WasmInstance, error) {
	wasmBytes, err := w.qe.GetWasmBytesByBinaryName(wasmBinaryName)
	if err != nil {
		return nil, err
	}

	// Compiles the module
	module, err := wasmer.NewModule(w.store, wasmBytes)
	if err != nil {
		return nil, err
	}

	// todo，let user decide which config to use? i think it's not necessary
	// Instantiates the module
	wasiEnv, err := wasmer.NewWasiStateBuilder("wasi-program").
		// Choose according to your actual situation
		// Argument("--foo").
		// Environment("ABC", "DEF").
		// MapDirectory("./", ".").
		Finalize()
	if err != nil {
		return nil, err
	}
	importObject, err := wasiEnv.GenerateImportObject(w.store, module)
	if err != nil {
		return nil, err
	}

	instance, err := wasmer.NewInstance(module, importObject)
	if err != nil {
		return nil, err
	}
	return &WasmerInstance{instance: instance}, nil
}

type WasmerInstance struct {
	instance *wasmer.Instance
}

func (ins *WasmerInstance) RunWASMPlugin(args *WasmPluginExchange) (*WasmPluginExchange, error) {
	// todo, use const?
	writeBuf, _ := ins.instance.Exports.GetFunction("writeBuf")
	readBuf, _ := ins.instance.Exports.GetFunction("readBuf")
	clearBuf, _ := ins.instance.Exports.GetFunction("clearBuf")
	getBufLen, _ := ins.instance.Exports.GetFunction("getBufLen")
	wasmerGuestFunc, _ := ins.instance.Exports.GetFunction("wasmerGuestFunc")

	err := sendStructToWASI(args, clearBuf, writeBuf)
	if err != nil {
		return nil, err
	}
	_, err = wasmerGuestFunc()
	if err != nil {
		return nil, err
	}

	return getStructFromWASI(getBufLen, readBuf)
}

func sendStructToWASI(args *WasmPluginExchange, clearBuf, writeBuf wasmer.NativeFunction) error {
	_, err := clearBuf()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(args)
	if err != nil {
		return err
	}

	bytesCount := len(bytes)
	overBytesCount := bytesCount % 8
	bytesCount = bytesCount - overBytesCount
	for idx := 0; idx < bytesCount; idx += 8 {
		data := binary.LittleEndian.Uint64(bytes[idx : idx+8])
		_, err = writeBuf(int64(data), int64(8))
		if err != nil {
			return err
		}
	}

	last8Bytes := make([]byte, 8)
	for idx := 0; idx < overBytesCount; idx++ {
		last8Bytes[idx] = bytes[bytesCount+idx]
	}

	data := binary.LittleEndian.Uint64(last8Bytes)
	_, err = writeBuf(int64(data), int64(overBytesCount))
	if err != nil {
		return err
	}
	return nil
}

func getStructFromWASI(getBufLen, readBuf wasmer.NativeFunction) (*WasmPluginExchange, error) {
	bytesCountI, err := getBufLen()
	if err != nil {
		return nil, err
	}
	bytesCount := bytesCountI.(int64)

	overBytesCount := bytesCount % 8
	bytesCount = bytesCount - overBytesCount
	bytes := make([]byte, bytesCount)
	for idx := int64(0); idx < bytesCount; idx += 8 {
		data, err := readBuf(idx)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint64(bytes[idx:], uint64(data.(int64)))
	}

	data, err := readBuf(bytesCount)
	if err != nil {
		return nil, err
	}
	lastBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastBytes, uint64(data.(int64)))
	lastBytes = lastBytes[:overBytesCount]
	bytes = append(bytes, lastBytes...)

	rst := WasmPluginExchange{}
	err = json.Unmarshal(bytes, &rst)
	if err != nil {
		return nil, err
	}
	return &rst, nil
}
