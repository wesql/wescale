/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package wasm

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	TestDatabaseName = "wasm_plugin_test_db"
)

func TestJobControllerBasic(t *testing.T) {
	t.Run("create user db", createUserDB)
	t.Run("module var test", moduleVarTest)
	t.Run("tablet var test", tabletVarTest)
	t.Run("data masking test", dataMaskingTest)
	t.Run("intercept", intercept)
}

func createUserDB(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("create database if not exists %s", TestDatabaseName))
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("CREATE TABLE %s.t1 (id INT NOT NULL AUTO_INCREMENT, int1 INT NOT NULL, int2 INT NOT NULL, name VARCHAR(255), PRIMARY KEY(id));", TestDatabaseName))
	require.Nil(t, err)
	time.Sleep(3 * time.Second)
	vtParams.DbName = TestDatabaseName
	CheckTableExist(t, &vtParams, fmt.Sprintf("%s.t1", TestDatabaseName))
}

func moduleVarTest(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: TestDatabaseName,
	}

	// 1. create a wasm plugin filter: add 1 in module count variables without lock every time, and set the query to "select $(count) as value"
	_, err := VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	var mu sync.Mutex
	var wg sync.WaitGroup

	// 2. because we don't require locks before adding counts, the final value should be less than 500
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			vtParams := mysql.ConnParams{
				Host:   clusterInstance.Hostname,
				Port:   clusterInstance.VtgateMySQLPort,
				DbName: TestDatabaseName,
			}
			ctx := context.Background()
			conn, err := mysql.Connect(ctx, &vtParams)
			require.Nil(t, err)
			mu.Unlock()

			for j := 0; j < 100; j++ {
				_, err = conn.ExecuteFetch(fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName), math.MaxInt64, false)
				require.Nil(t, err)
			}

			conn.Close()
		}()
	}

	wg.Wait()

	// 3. the filter will match insert type sql and replace the sql with "select $(count) as value",
	// so there should be no rows in the table
	qr, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("select count(*) as r from %s.t1", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	rows, err := qr.Named().Rows[0].ToInt64("r")
	require.Nil(t, err)
	require.Equal(t, int64(0), rows)

	// 4. send a insert query to get the count value, the count should be less than 500 + 1
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err := qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Less(t, count, int64(5*100)+1)
	fmt.Printf("the count is %d\n", count)

	// 5. create 2 new filters with the same module, because module variable is shared among filters using same module name,
	// and every filter will add 1 to the count variable, so now should add 3 to the count variable
	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1WithoutLockBak2)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1WithoutLockBak3)
	require.Nil(t, err)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	newCount, err := qr.Named().Rows[0].ToInt64("value")
	fmt.Printf("the newCount is %d\n", newCount)
	require.Nil(t, err)
	require.Equal(t, count+3, newCount)

	// 6. now we drop the one filter using this module
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	// 7. send a insert query to get the count value, the count value should be added 2 instead of 3 now
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	newCount2, err := qr.Named().Rows[0].ToInt64("value")
	fmt.Printf("the newCount2 is %d\n", newCount2)
	require.Nil(t, err)
	require.Equal(t, newCount+2, newCount2)

	// 8. now we drop the other 2 filters using this module
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestModuleKVInc1WithoutLockBak2)
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestModuleKVInc1WithoutLockBak3)
	require.Nil(t, err)

	// 9. add a new filter using the same wasm binary (module) name, the count value should start from 0 now
	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err = qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count, int64(1))

	// 10. we drop this filter and create a new filter using module that add 1 after requiring LOCK
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestModuleKVInc1Lock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1Lock)
	require.Nil(t, err)

	// 11. because we require locks before adding counts, the final value should equal 500
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			vtParams := mysql.ConnParams{
				Host:   clusterInstance.Hostname,
				Port:   clusterInstance.VtgateMySQLPort,
				DbName: TestDatabaseName,
			}
			ctx := context.Background()
			conn, err := mysql.Connect(ctx, &vtParams)
			require.Nil(t, err)
			mu.Unlock()

			for j := 0; j < 100; j++ {
				_, err = conn.ExecuteFetch(fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName), math.MaxInt64, false)
				require.Nil(t, err)
			}

			conn.Close()
		}()
	}

	wg.Wait()

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err = qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count, int64(5*100)+1)
	fmt.Printf("lock: the count is %d\n", count)

	// 12. the value in another tablet should be 0
	vtParams.DbName = TestDatabaseName + "@replica"
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err = qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count, int64(1))
	fmt.Printf("lock: the replica count is %d\n", count)
	vtParams.DbName = TestDatabaseName

	// 13. delete all filters and modules
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestModuleKVInc1Lock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, "delete from mysql.wasm_binary;")
	require.Nil(t, err)
}

func tabletVarTest(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: TestDatabaseName,
	}

	// 1. create a wasm plugin filter: add 1 in global count variables without lock every time, and set the query to "select $(count) as value"
	_, err := VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestTabletKVInc1WithoutLock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestTabletKVInc1WithoutLock)
	require.Nil(t, err)

	var mu sync.Mutex
	var wg sync.WaitGroup

	// 2. because we don't require locks before adding counts, the final value should be less than 500
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			vtParams := mysql.ConnParams{
				Host:   clusterInstance.Hostname,
				Port:   clusterInstance.VtgateMySQLPort,
				DbName: TestDatabaseName,
			}
			ctx := context.Background()
			conn, err := mysql.Connect(ctx, &vtParams)
			require.Nil(t, err)
			mu.Unlock()

			for j := 0; j < 100; j++ {
				_, err = conn.ExecuteFetch(fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName), math.MaxInt64, false)
				require.Nil(t, err)
			}

			conn.Close()
		}()
	}

	wg.Wait()

	// 3. the filter will match insert type sql and replace the sql with "select $(count) as value",
	// so there should be no rows in the table
	qr, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("select count(*) as r from %s.t1", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	rows, err := qr.Named().Rows[0].ToInt64("r")
	require.Nil(t, err)
	require.Equal(t, int64(0), rows)

	// 4. send a insert query to get the count value, the count should be less than 500 + 1
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err := qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Less(t, count, int64(5*100)+1)
	fmt.Printf("the count is %d\n", count)

	// 5. create 2 new filters with the same module, because module variable is shared among filters using same module name,
	// and every filter will add 1 to the count variable, so now should add 3 to the count variable
	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestTabletKVInc1WithoutLockBak2)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestTabletKVInc1WithoutLockBak3)
	require.Nil(t, err)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	newCount, err := qr.Named().Rows[0].ToInt64("value")
	fmt.Printf("the newCount is %d\n", newCount)
	require.Nil(t, err)
	require.Equal(t, count+3, newCount)

	// 6. now we drop the one filter using this module
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestTabletKVInc1WithoutLock)
	require.Nil(t, err)

	// 7. send a insert query to get the count value, the count value should be added 2 instead of 3 now
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	newCount2, err := qr.Named().Rows[0].ToInt64("value")
	fmt.Printf("the newCount2 is %d\n", newCount2)
	require.Nil(t, err)
	require.Equal(t, newCount+2, newCount2)

	// 8. now we drop the other 2 filters using this module
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestTabletKVInc1WithoutLockBak2)
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestTabletKVInc1WithoutLockBak3)
	require.Nil(t, err)

	// 9. add a new filter using the same wasm binary (module) name, the count value should NOT start from 0, because global var will be always keep in tablet memory, until it crash
	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestTabletKVInc1WithoutLock)
	require.Nil(t, err)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err = qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count, newCount2+1)

	// 10. we drop this filter and create a new filter using module that add 1 after requiring LOCK
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestTabletKVInc1WithoutLock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestTabletKVInc1Lock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestTabletKVInc1Lock)
	require.Nil(t, err)

	// 11. because we require locks before adding counts, the final value should equal 500
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			vtParams := mysql.ConnParams{
				Host:   clusterInstance.Hostname,
				Port:   clusterInstance.VtgateMySQLPort,
				DbName: TestDatabaseName,
			}
			ctx := context.Background()
			conn, err := mysql.Connect(ctx, &vtParams)
			require.Nil(t, err)
			mu.Unlock()

			for j := 0; j < 100; j++ {
				_, err = conn.ExecuteFetch(fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName), math.MaxInt64, false)
				require.Nil(t, err)
			}

			conn.Close()
		}()
	}

	wg.Wait()

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	newCount3, err := qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count+int64(5*100)+1, newCount3)
	fmt.Printf("lock: the count is %d\n", newCount3)

	// 12. the value in another tablet should be 0
	vtParams.DbName = TestDatabaseName + "@replica"
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err = qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Equal(t, count, int64(1))
	fmt.Printf("lock: the replica count is %d\n", count)
	vtParams.DbName = TestDatabaseName

	// 13. delete all filters and modules
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestTabletKVInc1Lock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, "delete from mysql.wasm_binary;")
	require.Nil(t, err)
}

func dataMaskingTest(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: TestDatabaseName,
	}

	// 1.insert 1 row data in table t1, the content of query result is what we just inserted
	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	qr, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("select * from %s.t1;", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	require.Equal(t, "Data", qr.Named().Rows[0].AsString("name", ""))

	// 2. create a wasm plugin data_masking: replace content of column with string type in query result to '****'
	_, err = VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestDataMasking)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestDataMasking)
	require.Nil(t, err)

	// 3. query again, the content of column name should be replaced by '****'
	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("select * from %s.t1;", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	require.Equal(t, "****", qr.Named().Rows[0].AsString("name", ""))

	// 4.content of other type column should not be changed
	require.Equal(t, int64(1), qr.Named().Rows[0].AsInt64("int1", 0))
	require.Equal(t, int64(100), qr.Named().Rows[0].AsInt64("int2", 0))

	// 5.drop filter and delete data in table t1
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("delete from %s.t1;", TestDatabaseName))
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestDataMasking)
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, "delete from mysql.wasm_binary")
	require.Nil(t, err)
}

func intercept(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: TestDatabaseName,
	}

	// 1.insert 1 row data in table t1
	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)

	// 2. create a wasm plugin intercepter: intercept update and delete sql without where clause
	_, err = VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestIntercepter)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestIntercepter)
	require.Nil(t, err)

	// 3. try to delete or update without where clause, should get error
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update %s.t1 set name = '123';", TestDatabaseName))
	require.NotNil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("delete from %s.t1;", TestDatabaseName))
	require.NotNil(t, err)

	// 4.drop filter, update and delete without where clause should be ok
	_, err = VtgateExecQuery(t, &vtParams, DropWasmFilterTestIntercepter)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update %s.t1 set name = '123';", TestDatabaseName))
	require.Nil(t, err)

	qr, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("select * from %s.t1;", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	require.Equal(t, "123", qr.Named().Rows[0].AsString("name", ""))

	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("delete from %s.t1;", TestDatabaseName))
	require.Nil(t, err)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("select * from %s.t1;", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 0, len(qr.Named().Rows))

	_, err = VtgateExecQuery(t, &vtParams, "delete from mysql.wasm_binary")
	require.Nil(t, err)
}

func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string) (*sqltypes.Result, error) {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	return qr, err
}

func CheckTableExist(t *testing.T, vtParams *mysql.ConnParams, tableName string) bool {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(fmt.Sprintf("show tables like '%s'", tableName), math.MaxInt64, true)
	require.Nil(t, err)
	return len(qr.Rows) > 0
}
