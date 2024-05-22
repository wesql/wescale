package tabletserver

import (
	"context"
	"fmt"
)

const wasmBinaryTableName = "mysql.wasm_binary"

func getQueryByName(wasmBinaryName string) string {
	return fmt.Sprintf("select * from %s where name = '%s'", wasmBinaryTableName, wasmBinaryName)
}

func SetQueryToQre(qre *QueryExecutor, query string) (err error) {
	qre.query = query
	qre.plan, err = qre.tsv.qe.GetPlan(context.Background(), qre.logStats, "", query, skipQueryPlanCache(nil))
	return err
}
