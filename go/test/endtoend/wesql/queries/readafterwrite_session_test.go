/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestReadAfterWrite_Session(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, false, false)
}

func TestReadAfterWrite_Session_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, false)
}

func TestReadAfterWrite_Session_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, true)
}

func TestReadAfterWrite_Instance(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, false, false)
}

func TestReadAfterWrite_Instance_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, false)
}

func TestReadAfterWrite_Instance_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, true)
}

func runReadAfterWriteTest(t *testing.T, enableReadWriteSplitting bool, readAfterWriteConsistency string, separateConn, enableTransaction bool, olap bool) {
	createDbExecDropDb(t, "readafterwrite_session_test", func(getConn func() *mysql.Conn) {
		rwConn := getConn()
		roConn := rwConn
		if separateConn {
			roConn = getConn()
		}
		execMulti(t, rwConn, "create table t1(c1 int primary key auto_increment, c2 int);insert into t1(c1, c2) values(null, 1)")

		// enable read after write & enable read after write for session
		if enableReadWriteSplitting {
			utils.Exec(t, roConn, "set session read_write_splitting_policy='random'")
		}
		utils.Exec(t, roConn, fmt.Sprintf("set @@read_after_write_consistency='%s'", readAfterWriteConsistency))
		if olap {
			utils.Exec(t, roConn, "set @@workload='OLAP'")
		}

		for i := 0; i < 1000; i++ {
			if enableTransaction {
				utils.Exec(t, rwConn, "begin")
			}
			result := utils.Exec(t, rwConn, "insert into t1(c1, c2) values(null, 1)")
			if enableTransaction {
				utils.Exec(t, rwConn, "commit")
			}
			lastInsertID := result.InsertID
			qr := utils.Exec(t, roConn, "select c1 from t1 order by c1 desc limit 1")
			if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
				t.Fatalf("read_after_write get empty result")
			}
			c1Val, err := qr.Rows[0][0].ToUint64()
			if err != nil {
				t.Fatalf("ToUint64 failed: %v", err)
			}
			if lastInsertID != c1Val {
				fmt.Printf("lastInsertID=%d\n", lastInsertID)
				fmt.Printf("c1=%d\n", c1Val)
				fmt.Printf("------------------------\n")
			}
		}
	})
}

func TestReadAfterWrite_Settings(t *testing.T) {
	backendPrimaryMysqlConn := getBackendPrimaryMysqlConn()
	defer backendPrimaryMysqlConn.Close()
	qr := utils.Exec(t, backendPrimaryMysqlConn, "select @@gtid_mode")
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		t.Fatalf("read_after_write get empty result")
	}
	gtidMode := qr.Rows[0][0].ToString()
	if gtidMode == "ON" {
		utils.Exec(t, backendPrimaryMysqlConn, "set global gtid_mode='ON_PERMISSIVE'")

		qr = utils.Exec(t, backendPrimaryMysqlConn, "select @@gtid_mode")
		if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
			t.Fatalf("read_after_write get empty result")
		}
		gtidMode = qr.Rows[0][0].ToString()
		if gtidMode != "ON_PERMISSIVE" {
			t.Fatalf("gtid_mode is not ON_PERMISSIVE")
		}
	}

	vtGateMysqlConn := getVTGateMysqlConn()
	defer vtGateMysqlConn.Close()

	_, err := utils.ExecAllowError(t, vtGateMysqlConn, "set @@read_after_write_consistency='INSTANCE'")
	assert.Error(t, err, "ReadAfterWriteConsistency can only be set when gtid_mode is ON")
}
