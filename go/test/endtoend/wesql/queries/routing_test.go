package queries

import (
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestRoutingTabletType(t *testing.T) {
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {
		utils.Exec(t, conn, "create database routingTest")
		defer utils.Exec(t, conn, "drop database routingTest")

		utils.Exec(t, conn, "create table routingTest.t (c1 int, c2 int, primary key(c1))")

		// insert should execute on primary
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "insert into routingTest.t(c1, c2) values (2, 2)")

		// select should execute on primary because read write splitting policy is DISABLE
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "select * from routingTest.t")

		// select should execute on replic or rdonly because read write splitting is enable (policy is random) and lb will pick between replic and rdonly
		utils.Exec(t, conn, "set session read_write_splitting_policy='random'")
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"replica or rdonly\")';")
		for i := 0; i < 10; i++ {
			utils.Exec(t, conn, "select * from routingTest.t")
		}

		// select should execute on primary because it's in txn
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "start transaction")
		utils.Exec(t, conn, "select * from routingTest.t")
		utils.Exec(t, conn, "commit")

		// select should execute on primary, though it's in read only txn, enable_read_write_splitting_for_read_only_txn is false
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "start transaction read only")
		utils.Exec(t, conn, "select * from routingTest.t")
		utils.Exec(t, conn, "commit")

		// select should execute on replica, it's in read only txn, enable_read_write_splitting_for_read_only_txn is true,
		// and read only txn should execute on single vttablet to guarantee correctness
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"replica\")';")
		utils.Exec(t, conn, "set session enable_read_write_splitting_for_read_only_txn=true")
		utils.Exec(t, conn, "start transaction read only")
		for i := 0; i < 10; i++ {
			utils.Exec(t, conn, "select * from routingTest.t")
		}
		utils.Exec(t, conn, "commit")

		// select should execute on primary because read write splitting policy is DISABLE
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "set session read_write_splitting_policy='disable'")
		utils.Exec(t, conn, "select * from routingTest.t")

		// select should execute on rdonly because keyspace tablet type is set
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"rdonly\")';")
		utils.Exec(t, conn, "use routingTest@RDONLY")
		utils.Exec(t, conn, "select * from t")

		// select should execute on primary because user hint tablet type is set
		utils.Exec(t, conn, "set @put_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType=return(\"primary\")';")
		utils.Exec(t, conn, "select /*vt+ ROLE=PRIMARY */ * from t")

		utils.Exec(t, conn, "set @remove_failpoint='vitess.io/vitess/go/vt/vtgate/AssertRoutingTabletType';")
	})
}
