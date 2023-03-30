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

	"github.com/stretchr/testify/assert"
)

func TestCRUD(t *testing.T) {
	createT1 := `CREATE TABLE t1 (
					c1 BIGINT NOT NULL,
					c2 BIGINT NOT NULL,
					c3 BIGINT,
					c4 varchar(100),
					PRIMARY KEY (c1),
					UNIQUE KEY (c2),
					UNIQUE KEY (c3),
					UNIQUE KEY (c4)
				) ENGINE=Innodb;`

	execWithConn(t, DefaultKeyspaceName, func(conn *mysql.Conn) {
		defer utils.Exec(t, conn, `drop table t1`)
		utils.Exec(t, conn, createT1)
		utils.AssertContainsError(t, conn, " \t; \n;", "Query was empty")
		execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
		utils.Exec(t, conn, `insert into t1(c1, c2, c3, c4) values (500,500,500,'abce');`)
		utils.Exec(t, conn, `update t1 set c1=400 where c1=300`)
		utils.Exec(t, conn, `delete from t1 where c1=500`)
		utils.AssertMatches(t, conn, `select c1,c2,c3 from t1 order by c1 desc`, `[[INT64(400) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
	})
}

func TestCrossDbDdlAndJoin(t *testing.T) {
	execWithConn(t, DefaultKeyspaceName, func(conn *mysql.Conn) {
		defer utils.Exec(t, conn, `drop table t1`)
		utils.Exec(t, conn, "create table t1 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into t1(c1, c2) values (1, 2)")

		utils.Exec(t, conn, "create database wesql2")
		defer utils.Exec(t, conn, "drop database wesql2")
		utils.Exec(t, conn, "create table wesql2.t2 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into wesql2.t2(c1, c2) values (1, 2)")

		utils.Exec(t, conn, "create database wesql3")
		defer utils.Exec(t, conn, "drop database wesql3")
		utils.Exec(t, conn, "create table wesql3.t3 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (1, 2)")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (3, 4)")
		utils.Exec(t, conn, "update wesql3.t3 set c1=5 where c1=3")
		utils.Exec(t, conn, "delete from wesql3.t3 where c1=5")

		qr := utils.Exec(t, conn, "select * from t1 join wesql2.t2 join wesql3.t3")
		assert.Equal(t, `[[INT32(1) INT32(2) INT32(1) INT32(2) INT32(1) INT32(2)]]`, fmt.Sprintf("%v", qr.Rows))
	})
}

func TestTransaction(t *testing.T) {
	execWithConn(t, DefaultKeyspaceName, func(conn *mysql.Conn) {
		utils.Exec(t, conn, "create table t1 (c1 int, c2 int, primary key(c1))")
		defer utils.Exec(t, conn, `drop table t1`)
		utils.Exec(t, conn, "insert into t1(c1, c2) values (1, 2)")

		utils.Exec(t, conn, "begin")

		utils.Exec(t, conn, "insert into t1(c1, c2) values (2, 3)")
		utils.Exec(t, conn, "insert into t1(c1, c2) values (3, 4)")

		utils.Exec(t, conn, "commit")

		qr := utils.Exec(t, conn, "select * from t1")
		assert.Equal(t, `[[INT32(1) INT32(2)] [INT32(2) INT32(3)] [INT32(3) INT32(4)]]`, fmt.Sprintf("%v", qr.Rows))
	})
}
