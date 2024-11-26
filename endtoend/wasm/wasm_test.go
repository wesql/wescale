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
	defer framework.UninstallWasm(t, wasmName, cluster.WescaleDb)

	filterBuilder := framework.NewWasmFilterBuilder(filterName, wasmName).SetPlans("Select")
	framework.CreateFilterIfNotExists(t, filterBuilder, cluster.WescaleDb)
	defer framework.DropFilter(t, filterName, cluster.WescaleDb)
	framework.AssertFilterExists(t, filterName, cluster.WescaleDb)

	framework.ExecNoError(t, cluster.WescaleDb, "create table wasm_e2e_test.test (id int, name varchar(255))")
	defer framework.ExecNoError(t, cluster.WescaleDb, "drop table wasm_e2e_test.test")
	framework.ExecNoError(t, cluster.WescaleDb, "insert into wasm_e2e_test.test values (1, 'test')")
	rows := framework.QueryNoError(t, cluster.WescaleDb, "select * from wasm_e2e_test.test")
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

//go:embed testdata/interceptor.wasm
var interceptorBytes []byte

func TestInterceptorPlugin(t *testing.T) {
	filterName := "interceptor"
	wasmName := "interceptor"
	framework.InstallWasm(t, interceptorBytes, wasmName, cluster.WescaleDb)
	defer framework.UninstallWasm(t, wasmName, cluster.WescaleDb)

	filterBuilder := framework.NewWasmFilterBuilder(filterName, wasmName).SetPlans("Update,Delete")
	framework.CreateFilterIfNotExists(t, filterBuilder, cluster.WescaleDb)
	defer framework.DropFilter(t, filterName, cluster.WescaleDb)
	framework.AssertFilterExists(t, filterName, cluster.WescaleDb)

	framework.ExecNoError(t, cluster.WescaleDb, "create table wasm_e2e_test.test (id int, name varchar(255))")
	defer framework.ExecNoError(t, cluster.WescaleDb, "drop table wasm_e2e_test.test")
	framework.ExecNoError(t, cluster.WescaleDb, "insert into wasm_e2e_test.test values (1, 'test')")
	rows := framework.QueryNoError(t, cluster.WescaleDb, "select * from wasm_e2e_test.test")
	defer rows.Close()

	// Expect the row to be unchanged
	framework.ExecWithErrorContains(t, cluster.WescaleDb, "no where clause", "update wasm_e2e_test.test set name = 'test2'")
	// Expect the row to be unchanged
	framework.ExecNoError(t, cluster.WescaleDb, "update wasm_e2e_test.test set name = 'test2' where id = 1")

	// Expect the row to be unchanged
	framework.ExecWithErrorContains(t, cluster.WescaleDb, "no where clause", "delete from wasm_e2e_test.test")
	// Expect the row to be deleted
	framework.ExecNoError(t, cluster.WescaleDb, "delete from wasm_e2e_test.test where id = 1")
}
