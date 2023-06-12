/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestCompreeGtidOverTime(t *testing.T) {
	execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		utils.Exec(t, conn, "create table t1 (c1 int PRIMARY KEY AUTO_INCREMENT, c2 int)")
	})
	defer execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		utils.Exec(t, conn, `drop table t1`)
	})
	allow := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			allow <- true
			utils.Exec(t, conn, "insert into t1 values(null,1)")
		}
		//wait all tablet heartbeat then call CompressGtid
		time.Sleep(time.Second * 7)
		qr := utils.Exec(t, conn, "show lastseengtid")
		assert.Truef(t, len(fmt.Sprintf("%v", qr.Rows[0])) < 100, "lastseengtid : %v ", qr.Rows[0])
		//t.Logf("qr: %v len: %v", qr.Rows[0], len(fmt.Sprintf("%v", qr.Rows[0])))
	})
	wg.Add(1)
	go execWithConnByVtgate(t, DefaultKeyspaceName, 2, func(conn *mysql.Conn) {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			<-allow
			utils.Exec(t, conn, "insert into t1 values(null,1)")
		}
		//wait all tablet heartbeat then call CompressGtid
		time.Sleep(time.Second * 7)
		qr := utils.Exec(t, conn, "show lastseengtid")
		assert.Truef(t, len(fmt.Sprintf("%v", qr.Rows[0])) < 100, "lastseengtid : %v ", qr.Rows[0])
		//t.Logf("qr: %v len: %v", qr.Rows[0], len(fmt.Sprintf("%v", qr.Rows[0])))
	})
	wg.Wait()
}
