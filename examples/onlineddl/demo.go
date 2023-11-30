/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

func main() {
	createDatabaseIfNotExists("root@tcp(127.0.0.1:15306)/mysql", "d1")
	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online", `create table t1(
		c1 bigint primary key auto_increment, 
		c2 int not null default 1,
		c3 int not null default 0
	)`)

	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 values (null, 1, 1)")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")

	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")

	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")

	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")
	execute("root@tcp(127.0.0.1:15306)/d1", "insert into t1 select null, c2, c3 from t1")

	//When there is no traffic at the source end, execute a normal OnlineDDL.
	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online", "alter table t1 add column c4 int not null default 0")

	//When there is continuous traffic at the source end, execute DDL.
	go continuousInsertData("root@tcp(127.0.0.1:15306)/d1", "insert into t1 values (null, 0, 0, 0)", 5, 60*time.Second)
	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online", "alter table t1 add column c5 int not null default 0")

	uuid, err := executeOnlineDDL("root@tcp(127.0.0.1:15306)/d1", "online --postpone-completion", "alter table t1 add column c6 int default 0")
	if err != nil {
		panic(err.Error())
	}
	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online --postpone-completion", fmt.Sprintf("alter vitess_migration '%s' complete", uuid))

	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online --prefer-instant-ddl", "alter table t1 add column c7 int default 0")

	executeOnlineDDLThenWaitForCompletion("root@tcp(127.0.0.1:15306)/d1", "online --declarative", "CREATE TABLE `t1` (  `c1` bigint NOT NULL AUTO_INCREMENT,  `c2` int NOT NULL DEFAULT '0',\n  `c3` int NOT NULL DEFAULT '0',\n  `c4` char NOT NULL DEFAULT '0',\n  `c6` int NOT NULL DEFAULT '0',\n  PRIMARY KEY (`c1`),\n  KEY(c2)\n) ENGINE=InnoDB AUTO_INCREMENT=5232588 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
}
