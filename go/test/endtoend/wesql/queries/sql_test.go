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

func TestCrossDbCRUD(t *testing.T) {
	execWithConn(t, DefaultKeyspaceName, func(conn *mysql.Conn) {
		defer utils.Exec(t, conn, `drop table t1`)
		utils.Exec(t, conn, "create table t1 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into t1(c1, c2) values (1, 1)")

		utils.Exec(t, conn, "create database wesql2")
		defer utils.Exec(t, conn, "drop database wesql2")
		utils.Exec(t, conn, "create table wesql2.t2 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into wesql2.t2(c1, c2) values (2, 2)")
		utils.Exec(t, conn, "insert into wesql2.t2(c1, c2) values (22, 22)")

		utils.Exec(t, conn, "create database wesql3")
		defer utils.Exec(t, conn, "drop database wesql3")
		utils.Exec(t, conn, "create table wesql3.t3 (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (3, 3)")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (33, 33)")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (330, 330)")
		utils.Exec(t, conn, "insert into wesql3.t3(c1, c2) values (3333, 3333)")
		utils.Exec(t, conn, "update wesql3.t3 set c1=333, c2=333 where c1=330")
		utils.Exec(t, conn, "delete from wesql3.t3 where c1=3333")

		// cross db join
		qr := utils.Exec(t, conn, "select * from t1 join wesql2.t2 join wesql3.t3 order by t1.c1 asc limit 1")
		assert.Equal(t, `[[INT32(1) INT32(1) INT32(22) INT32(22) INT32(3) INT32(3)]]`, fmt.Sprintf("%v", qr.Rows))

		// cross db update
		updateSQL := `
			UPDATE wesql.t1 
			INNER JOIN wesql2.t2 
			INNER JOIN wesql3.t3 
			SET t1.c2 = 4, t2.c2 = 44, t3.c2 = 444`
		utils.Exec(t, conn, updateSQL)
		qr = utils.Exec(t, conn, "select * from t1 join wesql2.t2 join wesql3.t3 order by t1.c1 asc limit 1")
		assert.Equal(t, `[[INT32(1) INT32(4) INT32(22) INT32(44) INT32(3) INT32(444)]]`, fmt.Sprintf("%v", qr.Rows))

		// cross db delete
		utils.Exec(t, conn, "DELETE t1, t2 FROM wesql.t1 as t1 INNER JOIN wesql2.t2 as t2 WHERE t1.c2=t2.c2 or t1.c1=1")
		qr = utils.Exec(t, conn, "select count(*) from wesql.t1")
		assert.Equal(t, `[[INT64(0)]]`, fmt.Sprintf("%v", qr.Rows))
		qr = utils.Exec(t, conn, "select count(*) from wesql2.t2")
		assert.Equal(t, `[[INT64(0)]]`, fmt.Sprintf("%v", qr.Rows))
		qr = utils.Exec(t, conn, "select count(*) from wesql3.t3")
		assert.Equal(t, `[[INT64(3)]]`, fmt.Sprintf("%v", qr.Rows))
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

// TestCreateDropDatabaseWithTheSameNameMultipleTimes tests that we can create and drop a database with the same name multiple times.
func TestCreateDropDatabaseWithTheSameNameMultipleTimes(t *testing.T) {
	dbName := "wesql_test_db"
	execWithConn(t, DefaultKeyspaceName, func(conn *mysql.Conn) {
		utils.Exec(t, conn, fmt.Sprintf("create database %s", dbName))
		utils.AssertDatabaseExists(t, conn, dbName)
		utils.Exec(t, conn, fmt.Sprintf("drop database %s", dbName))
		utils.AssertDatabaseNotExists(t, conn, dbName)
		utils.Exec(t, conn, fmt.Sprintf("create database %s", dbName))
		utils.AssertDatabaseExists(t, conn, dbName)
		utils.Exec(t, conn, fmt.Sprintf("drop database %s", dbName))
		utils.AssertDatabaseNotExists(t, conn, dbName)
	})
}