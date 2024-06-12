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
	t.Run("global var test", globalVarTest)
}

func createUserDB(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("create database if not exists %s", TestDatabaseName))
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("CREATE TABLE %s.t1 (id INT NOT NULL AUTO_INCREMENT, int1 INT NOT NULL, int2 INT NOT NULL, Name VARCHAR(255), PRIMARY KEY(id));", TestDatabaseName))
	require.Nil(t, err)
	time.Sleep(3 * time.Second)
	vtParams.DbName = TestDatabaseName
	CheckTableExist(t, &vtParams, fmt.Sprintf("%s.t1", TestDatabaseName))
}

func globalVarTest(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: TestDatabaseName,
	}

	_, err := VtgateExecQuery(t, &vtParams, InsertWasmBinaryTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	_, err = VtgateExecQuery(t, &vtParams, CreateWasmFilterTestModuleKVInc1WithoutLock)
	require.Nil(t, err)

	var mu sync.Mutex
	var wg sync.WaitGroup

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

	qr, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("select count(*) as r from %s.t1", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	rows, err := qr.Named().Rows[0].ToInt64("r")
	require.Nil(t, err)
	require.Equal(t, int64(0), rows)

	qr, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("INSERT INTO %s.t1 (int1, int2,name) VALUES (1, 100, 'Data');", TestDatabaseName))
	require.Nil(t, err)
	require.Equal(t, 1, len(qr.Named().Rows))
	count, err := qr.Named().Rows[0].ToInt64("value")
	require.Nil(t, err)
	require.Less(t, count, int64(5*100))
	fmt.Printf("the count is %d\n", count)

	// create filter with some module
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
