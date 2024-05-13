package tabletserver

import (
	"log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

// todo, how to assin runtime better?
const WASMER = "wamser-go"
const WAZERO = "wazero"

var (
	DefaultRuntime = WAZERO
	Runtime        = WAZERO
)

// todo
func registerCclFlags(fs *pflag.FlagSet) {
	fs.StringVar(&DefaultRuntime, "default_wasm_runtime", DefaultRuntime, "the default runtime for wasm plugin")
	fs.StringVar(&Runtime, "wasm_runtime", Runtime, "the runtime for wasm plugin")
}

func init() {
	servenv.OnParseFor("vttablet", registerCclFlags)
}

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
	case WASMER:
		return initWasmerRuntime(qe)
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
	InitOrGetWasmInstance(key string, wasmBinaryName string) (WasmInstance, error)
	ClearWasmInstance()
}

type WasmInstance interface {
	RunWASMPlugin(args *WasmPluginExchange) (*WasmPluginExchange, error)
	RunWASMPluginAfter(args *WasmPluginExchangeAfter) (*WasmPluginExchangeAfter, error)
}
