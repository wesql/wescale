/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestReadAfterWrite_Session(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", false, false)
}

func TestReadAfterWrite_Session_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", true, false)
}

func TestReadAfterWrite_Session_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "SESSION", true, true)
}

func TestReadAfterWrite_Instance(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", false, false)
}

func TestReadAfterWrite_Instance_Transaction(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", false, false)
}

func TestReadAfterWrite_Instance_Transaction_OLAP(t *testing.T) {
	runReadAfterWriteTest(t, true, "INSTANCE", false, true)
}

func runReadAfterWriteTest(t *testing.T, enableReadWriteSplitting bool, readAfterWriteScope string, enableTransaction bool, olap bool) {
	createDbExecDropDb(t, "readafterwrite_session_test", func(getConn func() *mysql.Conn) {
		conn := getConn()
		execMulti(t, conn, "create table t1(c1 int primary key auto_increment, c2 int);insert into t1(c1, c2) values(null, 1)")

		// enable read after write & enable read after write for session
		if enableReadWriteSplitting {
			utils.Exec(t, conn, "set session read_write_splitting_policy='random'")
		}
		utils.Exec(t, conn, fmt.Sprintf("set @@read_after_write_scope='%s'", readAfterWriteScope))
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
			qr := utils.Exec(t, conn, "select c1 from t1 order by c1 desc limit 1")
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
