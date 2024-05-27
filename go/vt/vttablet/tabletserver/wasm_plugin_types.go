package tabletserver

import "vitess.io/vitess/go/sqltypes"

type WasmVM interface {
	GetRuntimeType() string
	InitRuntime() error
	GetWasmModule(key string) (bool, WasmModule)
	InitWasmModule(key string, wasmBytes []byte) (WasmModule, error)
	ClearWasmModule(key string)
	Close() error
}

type WasmModule interface {
	NewInstance(qre *QueryExecutor) (WasmInstance, error)
	Close() error
}

type WasmInstance interface {
	RunWASMPlugin() error
	RunWASMPluginAfter() error
	Close() error
	SetErrorMessage(message string)
	SetQueryResult(result *sqltypes.Result)
	GetQueryResult() *sqltypes.Result
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
