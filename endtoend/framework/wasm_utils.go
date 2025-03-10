package framework

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"fmt"
	"github.com/dsnet/compress/bzip2"
	"github.com/stretchr/testify/assert"
	"testing"
)

const wasmRuntime = "wazero"
const wasmCompressAlgorithm = "bzip2"

func InstallWasm(t *testing.T, wasmBytes []byte, wasmName string, db *sql.DB) {
	t.Helper()
	hash := calcMd5String32(wasmBytes)
	compressedWasmBytes, err := compressByBZip2(wasmBytes)
	assert.NoError(t, err)
	ExecNoError(t, db, `insert ignore into mysql.wasm_binary(name,runtime,data,compress_algorithm,hash_before_compress) values (?,?,?,?,?)`,
		wasmName, wasmRuntime, compressedWasmBytes, wasmCompressAlgorithm, hash)
}

func UninstallWasm(t *testing.T, wasmName string, db *sql.DB) {
	t.Helper()
	ExecNoError(t, db, `delete from mysql.wasm_binary where name = ?`, wasmName)
}

func calcMd5String32(data []byte) string {
	hash := md5.Sum(data)
	return fmt.Sprintf("%x", hash)
}

func compressByBZip2(originalData []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := bzip2.NewWriter(&buf, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(originalData); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
