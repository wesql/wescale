package binlogconsumer

import (
	"bytes"
	"compress/bzip2"
	"context"
	"crypto/md5"
	"fmt"
	"io"

	"vitess.io/vitess/go/internal/global"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

//todo cdc: refactor this file, it's duplicated with go/vt/vttablet/tabletserver/wasm_plugin_controller.go

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

const WasmBinaryTableName = "mysql.wasm_binary"
const wasmBinaryCompressAlgorithm = "bzip2"

func getQueryByName(wasmBinaryName string) string {
	return fmt.Sprintf("select * from %s where name = '%s'", WasmBinaryTableName, wasmBinaryName)
}

func GetWasmBytesByBinaryName(ctx context.Context, wasmBinaryName string, svc queryservice.QueryService) ([]byte, error) {
	query := getQueryByName(wasmBinaryName)
	target := &querypb.Target{
		Keyspace:   global.DefaultKeyspace,
		Shard:      global.DefaultShard,
		TabletType: topodata.TabletType_PRIMARY,
	}
	qr, err := svc.ExecuteInternal(ctx, target, query, nil, 0, 0, nil)
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
