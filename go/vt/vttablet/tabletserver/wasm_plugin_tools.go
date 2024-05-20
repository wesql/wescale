package tabletserver

import (
	"context"
	"fmt"
	"unsafe"
)

const wasmBinaryTableName = "mysql.wasm_binary"

func getQueryByName(wasmBinaryName string) string {
	return fmt.Sprintf("select * from %s where name = '%s'", wasmBinaryTableName, wasmBinaryName)
}

func ConvertQueryExecutorToWasmPluginExchange(qre *QueryExecutor) *WasmPluginExchange {
	// todo by newborn22
	return &WasmPluginExchange{Query: qre.query}
}

func ConvertWasmPluginExchangeToQueryExecutor(qre *QueryExecutor, exchange *WasmPluginExchange) {
	// todo by newborn22
	qre.plan, _ = qre.tsv.qe.GetPlan(context.Background(), qre.logStats, "", exchange.Query, skipQueryPlanCache(nil))
	//plan GetPlan(ctx, logStats, target.Keyspace, query, skipQueryPlanCache(options))
}

func SetQueryToQre(qre *QueryExecutor, query string) (err error) {
	qre.query = query
	qre.plan, err = qre.tsv.qe.GetPlan(context.Background(), qre.logStats, "", query, skipQueryPlanCache(nil))
	return err
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
