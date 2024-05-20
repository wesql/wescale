package tabletserver

import (
	"log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

// todo, how to assin runtime better?
// todo newborn22 5.14 2个runtime
const WASMER = "wamser-go"
const WAZERO = "wazero"

var (
	DefaultRuntime = WAZERO
	Runtime        = WAZERO
)

// todo
func registerWasmFlags(fs *pflag.FlagSet) {
	fs.StringVar(&DefaultRuntime, "default_wasm_runtime", DefaultRuntime, "the default runtime for wasm plugin")
	fs.StringVar(&Runtime, "wasm_runtime", Runtime, "the runtime for wasm plugin")
}

func init() {
	servenv.OnParseFor("vttablet", registerWasmFlags)
}

// todo newborn22 5.14 -> data struct to funciton
// 1.query
type WasmPluginExchange struct {
	Query string
}

// todo
type WasmPluginExchangeAfter struct {
	Query string
}

// todo，删除ctrl，只用runtime?
type WasmPluginController struct {
	Runtime WasmRuntime
}

// todo 确保单例
// todo 是否可以动态更换runtime?
func NewWasmPluginController(qe *QueryEngine) *WasmPluginController {
	return &WasmPluginController{
		Runtime: initWasmRuntime(qe),
	}
}

func initWasmRuntime(qe *QueryEngine) WasmRuntime {
	switch Runtime {
	//case WASMER:
	//	return initWasmerRuntime(qe)
	case WAZERO:
		return initWazeroRuntime(qe)
	default:
		// todo, init a default runtime or panic?
		log.Printf("runtime %v is not supported, use default runtime %v", Runtime, DefaultRuntime)
		Runtime = DefaultRuntime
		return initWasmRuntime(qe)
	}
}

type WasmRuntime interface {
	GetRuntimeType() string
	InitOrGetWasmModule(key string, wasmBinaryName string) (WasmModule, error)
	ClearWasmModule(key string)
	GetWasmInstance(key string, wasmBinaryName string, qre *QueryExecutor) (WasmInstance, error)
}

type WasmModule interface {
	NewInstance(qre *QueryExecutor) (WasmInstance, error)
}

type WasmInstance interface {
	RunWASMPlugin() error
	RunWASMPluginAfter(args *WasmPluginExchangeAfter) (*WasmPluginExchangeAfter, error)
}
