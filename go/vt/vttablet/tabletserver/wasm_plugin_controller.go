package tabletserver

import (
	"context"
	"fmt"
	"github.com/spf13/pflag"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/servenv"
)

const WASMER = "wamser-go"
const WAZERO = "wazero"

var (
	RuntimeType = WAZERO
)

func registerWasmFlags(fs *pflag.FlagSet) {
	fs.StringVar(&RuntimeType, "wasm_runtime_type", WAZERO, "the runtime for wasm plugin. default is wazero.")
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

type WasmPluginController struct {
	qe *QueryEngine
	VM WasmVM
}

func NewWasmPluginController(qe *QueryEngine) *WasmPluginController {
	return &WasmPluginController{
		qe: qe,
		VM: initWasmVM(),
	}
}

func (wpc *WasmPluginController) GetWasmBytesByBinaryName(ctx context.Context, wasmBinaryName string) ([]byte, error) {
	query := getQueryByName(wasmBinaryName)
	qr, err := wpc.qe.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get wasm binary by name %s failed : %v", wasmBinaryName, err)
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("get wasm binary by name %s failed : qr len is %v instead of 1", wasmBinaryName, len(qr.Named().Rows))
	}
	binaryStr, err := qr.Named().Rows[0].ToString("data")
	if err != nil {
		return nil, err
	}

	// todo by newborn22,split into small func and test correctness
	byteStrArray := strings.Split(binaryStr, " ")
	bytes := make([]byte, 0)
	for _, byteInt := range byteStrArray {
		b, err := strconv.ParseUint(byteInt, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("error when parsing action args: %v", err)
		}
		bytes = append(bytes, byte(b))
	}
	return bytes, nil
}

func initWasmVM() WasmVM {
	switch RuntimeType {
	//case WASMER:
	//	return initWasmerRuntime(qe)
	case WAZERO:
		return initWazeroVM()
	default:
		return initWazeroVM()
	}
}
