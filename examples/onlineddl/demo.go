/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

func main() {
	createDatabaseIfNotExists("root@tcp(127.0.0.1:15306)/mysql", "d2")
	executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online", "create table t1 (c1 int primary key auto_increment, c2 int)")

	continuousInsertData("root@tcp(127.0.0.1:15306)/d2", "insert into t1(c1, c2) values(null, 1)", 5, 300*time.Second)

	executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online", "alter table t1 add column c3 int default 0")

	uuid := executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online --postpone-completion", "alter table t1 add column c4 int default 0")
	executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online --postpone-completion", fmt.Sprintf("alter vitess_migration '%s' complete", uuid))

	executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online --prefer-instant-ddl", "alter table t1 add column c5 int default 0")

	executeOnlineDDL("root@tcp(127.0.0.1:15306)/d2", "online --declarative", "CREATE TABLE `t1` (  `c1` bigint NOT NULL AUTO_INCREMENT,\n  `c2` int NOT NULL DEFAULT '0',\n  `c3` int NOT NULL DEFAULT '0',\n  `c4` char NOT NULL DEFAULT '0',\n  `c6` int NOT NULL DEFAULT '0',\n  PRIMARY KEY (`c1`),\n  KEY(c2)\n) ENGINE=InnoDB AUTO_INCREMENT=5232588 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

}

func continuousInsertData(dsn string, insertSQL string, parallelism int, duration time.Duration) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		panic(err.Error())
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	insertFunc := func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			default:
				conn.ExecContext(cancelCtx, insertSQL)
			}
		}
	}

	for i := 1; i <= parallelism; i++ {
		go insertFunc()
	}
	time.Sleep(duration)
	cancel()
}
