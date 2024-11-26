package wasm

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"github.com/wesql/wescale/endtoend/framework"
	"testing"
)

//go:embed testdata/datamasking.wasm
var dataMaskingBytes []byte

func TestDataMaskingPlugin(t *testing.T) {
	filterName := "datamasking"
	wasmName := "datamasking"
	framework.InstallWasm(t, dataMaskingBytes, wasmName, cluster.WescaleDb)

	builder := framework.NewWasmFilterBuilder(filterName, wasmName).SetPlans("Select")
	framework.CreateFilterIfNotExists(t, builder, cluster.WescaleDb)
	defer framework.DropFilter(t, filterName, cluster.WescaleDb)
	framework.AssertFilterExists(t, filterName, cluster.WescaleDb)

	framework.Exec(t, cluster.WescaleDb, "create table wasm_e2e_test.test (id int, name varchar(255))")
	defer framework.Exec(t, cluster.WescaleDb, "drop table wasm_e2e_test.test")

	framework.Exec(t, cluster.WescaleDb, "insert into wasm_e2e_test.test values (1, 'test')")

	rows := framework.Query(t, cluster.WescaleDb, "select * from wasm_e2e_test.test")
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		assert.NoError(t, err)
		assert.NotEqual(t, 1, id)
		assert.Equal(t, "****", name)
	}
}
