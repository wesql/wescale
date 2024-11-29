package tabletserver

import (
	"bytes"
	"compress/bzip2"
	"context"
	"crypto/md5"
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	RuntimeType                 = WAZERO
	wasmBinaryCompressAlgorithm = "bzip2"
)

func registerWasmFlags(fs *pflag.FlagSet) {
	fs.StringVar(&RuntimeType, "wasm_runtime_type", WAZERO, "the runtime for wasm plugin. default is wazero. options: [wazero]")
}

func init() {
	servenv.OnParseFor("vttablet", registerWasmFlags)
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

func UnCompressByBZip2(compressedData []byte) ([]byte, error) {
	r := bzip2.NewReader(bytes.NewReader(compressedData))
	decompressedData, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decompressedData, nil
}

func CalcMd5String32(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("%x", hash)
}

func (wpc *WasmPluginController) GetWasmBytesByBinaryName(ctx context.Context, wasmBinaryName string) ([]byte, error) {
	query := generateWasmQueryByName(wasmBinaryName)
	qr, err := wpc.qe.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get wasm binary by name %s failed : %v", wasmBinaryName, err)
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("get wasm binary by name %s failed : qr len is %v instead of 1", wasmBinaryName, len(qr.Named().Rows))
	}

	bytes, err := qr.Named().Rows[0].ToBytes("data")
	if err != nil {
		return nil, fmt.Errorf("get wasm binary by name %s failed : uncompress data error: %v", wasmBinaryName, err)
	}

	compressAlgorithm := qr.Named().Rows[0].AsString("compress_algorithm", "")
	if compressAlgorithm != "" && compressAlgorithm != wasmBinaryCompressAlgorithm {
		return nil, fmt.Errorf("get wasm binary by name %s failed : compress algorithm is %v instead of %v",
			wasmBinaryName, compressAlgorithm, wasmBinaryCompressAlgorithm)
	}

	if compressAlgorithm == wasmBinaryCompressAlgorithm {
		bytes, err = UnCompressByBZip2(bytes)
		if err != nil {
			return nil, fmt.Errorf("get wasm binary by name %s failed : uncompress data error: %v", wasmBinaryName, err)
		}
	}

	hashInTable := qr.Named().Rows[0].AsString("hash_before_compress", "")
	if hashInTable != "" {
		hash := CalcMd5String32(bytes)
		if hash != hashInTable {
			return nil, fmt.Errorf("get wasm binary by name %s failed : hash is not equal", wasmBinaryName)
		}
	}

	return bytes, nil
}

func initWasmVM() WasmVM {
	switch RuntimeType {
	case WAZERO:
		return initWazeroVM()
	default:
		return initWazeroVM()
	}
}
