package tabletserver

import (
	"context"
	"fmt"
)

func getQueryByName(wasmBinaryName string) string {
	return fmt.Sprintf("select * from %s where name = '%s'", WasmBinaryTableName, wasmBinaryName)
}

func SetQueryToQre(qre *QueryExecutor, query string) (err error) {
	qre.query = query
	qre.plan, err = qre.tsv.qe.GetPlan(context.Background(), qre.logStats, "", query, skipQueryPlanCache(nil))
	return err
}
