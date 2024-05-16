package tabletserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

const wasmBinaryTableName = "mysql.wasm_binary"

func getQueryByName(wasmBinaryName string) string {
	return fmt.Sprintf("select * from %s where name = '%s'", wasmBinaryTableName, wasmBinaryName)
}

func (qe *QueryEngine) GetWasmBytesByBinaryName(wasmBinaryName string) ([]byte, error) {
	query := getQueryByName(wasmBinaryName)
	qr, err := qe.ExecuteQuery(context.Background(), query)
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

func ConvertQueryExecutorToWasmPluginExchange(qre *QueryExecutor) *WasmPluginExchange {
	// todo by newborn22
	return &WasmPluginExchange{Query: qre.query}
}

func ConvertWasmPluginExchangeToQueryExecutor(qre *QueryExecutor, exchange *WasmPluginExchange) {
	// todo by newborn22
	qre.plan, _ = qre.tsv.qe.GetPlan(context.Background(), qre.logStats, "d1", exchange.Query, skipQueryPlanCache(nil))
	//plan GetPlan(ctx, logStats, target.Keyspace, query, skipQueryPlanCache(options))
}

func ConvertQueryExecutorToWasmPluginExchangeAfter(qre *QueryExecutor) *WasmPluginExchangeAfter {
	// todo by newborn22
	return &WasmPluginExchangeAfter{Query: qre.query}
}

func ConvertWasmPluginExchangeToQueryExecutorAfter(qre *QueryExecutor, exchange *WasmPluginExchangeAfter) {
	// todo by newborn22
	qre.query = exchange.Query
}

func PtrToString(ptr uint32, size uint32) string {
	return unsafe.String((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}
