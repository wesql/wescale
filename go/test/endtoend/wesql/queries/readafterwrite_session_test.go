/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestReadAfterWrite_Session(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, false, false, false)
}

func TestReadAfterWrite_Session_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, false, false)
}

func TestReadAfterWrite_Session_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, true, false)
}

func TestReadAfterWrite_Instance(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, false, false, false)
}

func TestReadAfterWrite_Instance_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, false, false)
}

func TestReadAfterWrite_Instance_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, true, false)
}

func TestReadAfterWrite_Session_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, false, false, true)
}

func TestReadAfterWrite_Session_Transaction_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, false, true)
}

func TestReadAfterWrite_Session_Transaction_OLAP_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, true, true, true)
}

func TestReadAfterWrite_Instance_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, false, false, true)
}

func TestReadAfterWrite_Instance_Transaction_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, false, true)
}

func TestReadAfterWrite_Instance_Transaction_OLAP_WithTimeZone(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", true, true, true, true)
}

func TestReadAfterWrite_Global(t *testing.T) {
	runReadAfterWriteGlobalTest(t, true, "GLOBAL", true, false, false)
}

func TestReadAfterWrite_Global_Transaction(t *testing.T) {
	runReadAfterWriteGlobalTest(t, true, "GLOBAL", true, true, false)
}

func TestReadAfterWrite_Global_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteGlobalTest(t, true, "GLOBAL", true, true, true)
}

func TestReadAfterWrite_Global_Transaction_OLAP_CrossVTGate(t *testing.T) {
	runReadAfterWriteCrossVTGateTest(t, true, "GLOBAL", true, true)
}

func TestReadAfterWrite_Global_Transaction_CrossVTGate(t *testing.T) {
	runReadAfterWriteCrossVTGateTest(t, true, "GLOBAL", true, false)
}

func TestReadAfterWrite_Session_Transaction_OLAP_CrossVTGate_And_Except_Failure(t *testing.T) {
	runReadAfterWriteCrossVTGateTest(t, true, "SESSION", true, true)
}

func runReadAfterWriteTest(t *testing.T, enableReadWriteSplitting bool, readAfterWriteConsistency string, separateConn, enableTransaction bool, olap bool, setTimeZone bool) {
	createDbExecDropDb(t, "readafterwrite_session_test", func(getConn func() *mysql.Conn) {
		rwConn := getConn()
		roConn := rwConn
		if separateConn {
			roConn = getConn()
		}
		if setTimeZone {
			utils.Exec(t, rwConn, "set time_zone='+04:00'")
			utils.Exec(t, roConn, "set time_zone='+04:00'")
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
			assert.Equal(t, lastInsertID, c1Val, "lastInsertID(%#v) != c1Val(%#v)", lastInsertID, c1Val)
		}
	})
}

func runReadAfterWriteGlobalTest(t *testing.T, enableReadWriteSplitting bool, readAfterWriteConsistency string, separateConn, enableTransaction bool, olap bool) {
	createDbExecDropDb(t, "readafterwrite_session_test", func(getConn func() *mysql.Conn) {
		rwConn := getConn()
		roConn := rwConn
		if separateConn {
			roConn = getConn()
		}
		execMulti(t, rwConn, "create table t1(c1 int primary key auto_increment, c2 int);insert into t1(c1, c2) values(null, 1)")

		for i := 0; i < 1000; i++ {
			rwConn = getConn()
			roConn = rwConn
			if separateConn {
				roConn = getConn()
			}
			// enable read after write & enable read after write for session
			if enableReadWriteSplitting {
				utils.Exec(t, roConn, "set session read_write_splitting_policy='random'")
			}
			utils.Exec(t, roConn, fmt.Sprintf("set @@read_after_write_consistency='%s'", readAfterWriteConsistency))
			if olap {
				utils.Exec(t, roConn, "set @@workload='OLAP'")
			}
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
			assert.Equal(t, lastInsertID, c1Val, "lastInsertID(%#v) != c1Val(%#v)", lastInsertID, c1Val)
		}
	})
}

func runReadAfterWriteCrossVTGateTest(t *testing.T, enableReadWriteSplitting bool, readAfterWriteConsistency string, enableTransaction bool, olap bool) {
	execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		utils.Exec(t, conn, "create table t1 (c1 int PRIMARY KEY AUTO_INCREMENT, c2 int)")
	})
	defer execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `drop table t1`)
	})

	CrossVTGateFailedtimes := 0
	ch := make(chan uint64, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		defer wg.Done()
		// enable read after write & enable read after write for session
		if enableReadWriteSplitting {
			utils.Exec(t, conn, "set session read_write_splitting_policy='random'")
		}
		utils.Exec(t, conn, fmt.Sprintf("set @@read_after_write_consistency='%s'", readAfterWriteConsistency))
		if olap {
			utils.Exec(t, conn, "set @@workload='OLAP'")
		}

		for i := 0; i < 1000; i++ {
			if enableTransaction {
				utils.Exec(t, conn, "begin")
			}
			result := utils.Exec(t, conn, "insert into t1(c1, c2) values(null, 1)")
			if enableTransaction {
				utils.Exec(t, conn, "commit")
			}
			lastInsertID := result.InsertID
			ch <- lastInsertID
		}
	})
	go execWithConnByVtgate(t, DefaultKeyspaceName, 2, func(conn *mysql.Conn) {
		defer wg.Done()
		// enable read after write & enable read after write for session
		if enableReadWriteSplitting {
			utils.Exec(t, conn, "set session read_write_splitting_policy='random'")
		}
		utils.Exec(t, conn, fmt.Sprintf("set @@read_after_write_consistency='%s'", readAfterWriteConsistency))
		if olap {
			utils.Exec(t, conn, "set @@workload='OLAP'")
		}

		for i := 0; i < 1000; i++ {
			lastInsertID := <-ch
			qr := utils.Exec(t, conn, "select c1 from t1 order by c1 desc limit 1")
			if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
				if strings.Compare(strings.ToUpper(readAfterWriteConsistency), "GLOBAL") != 0 {
					CrossVTGateFailedtimes++
					continue
				} else {
					t.Fatalf("read_after_write get empty result, %#v", i)
				}
			}
			c1Val, err := qr.Rows[0][0].ToUint64()
			if err != nil {
				t.Fatalf("ToUint64 failed: %v", err)
			}

			if strings.Compare(strings.ToUpper(readAfterWriteConsistency), "GLOBAL") == 0 {
				// global, and except pass
				assert.LessOrEqual(t, lastInsertID, c1Val, "lastInsertID(%#v) != c1Val(%#v)", lastInsertID, c1Val)
			} else {
				// none global, and except to have errors occur
				if lastInsertID != c1Val {
					CrossVTGateFailedtimes++
				}
			}
		}
		if strings.Compare(strings.ToUpper(readAfterWriteConsistency), "GLOBAL") != 0 {
			assert.Greater(t, CrossVTGateFailedtimes, 0, "except failure: read after write cross vtgate when using non-global read after write strategy")
		}
	})
	wg.Wait()
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
