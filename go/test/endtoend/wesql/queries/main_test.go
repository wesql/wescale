/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"context"
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance     *cluster.LocalProcessCluster
	cell                = "zone1"
	hostname            = "localhost"
	KeyspaceName        = "_vt"
	DefaultKeyspaceName = "wesql"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	code := runAllTests(m)
	os.Exit(code)
}

func runAllTests(m *testing.M) int {
	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return 1
	}

	// Start keyspace
	Keyspace := &cluster.Keyspace{
		Name: KeyspaceName,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-transaction-timeout", "3", "--queryserver-config-max-result-size", "30"}
	if err := clusterInstance.StartUnshardedKeyspace(*Keyspace, 1, false); err != nil {
		log.Fatal(err.Error())
		return 1
	}

	// Start vtgate
	clusterInstance.VtGateExtraArgs = []string{
		"--planner-version=gen4",
		"--warn_sharded_only=true",
	}
	if err := clusterInstance.StartVtgate(); err != nil {
		log.Fatal(err.Error())
		return 1
	}

	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess
	if _, err := primaryTablet.QueryTablet("show databases", KeyspaceName, false); err != nil {
		log.Fatal(err.Error())
		return 1
	}

	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: KeyspaceName,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	if err != nil {
		log.Fatal(err.Error())
		return 1
	}
	defer conn.Close()
	_, err = conn.ExecuteFetch("create database if not exists "+DefaultKeyspaceName, 1000, false)
	if err != nil {
		log.Fatal(err.Error())
		return 1
	}

	return m.Run()
}

func execMulti(t *testing.T, conn *mysql.Conn, query string) []*sqltypes.Result {
	t.Helper()
	var res []*sqltypes.Result
	qr, more, err := conn.ExecuteFetchMulti(query, 1000, true)
	res = append(res, qr)
	require.NoError(t, err)
	for more == true {
		qr, more, _, err = conn.ReadQueryResult(1000, true)
		require.NoError(t, err)
		res = append(res, qr)
	}
	return res
}

func execWithConnWithoutDB(t *testing.T, f func(conn *mysql.Conn)) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	f(conn)
}

func execWithConn(t *testing.T, db string, f func(conn *mysql.Conn)) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: db,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	f(conn)
}

func createDbExecDropDb(t *testing.T, db string, f func(getConn func() *mysql.Conn)) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "create database "+db)
	defer utils.Exec(t, conn, "drop database "+db)

	getConn := func() *mysql.Conn {
		vtParams := mysql.ConnParams{
			Host:   "localhost",
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: db,
		}
		conn, err := mysql.Connect(ctx, &vtParams)
		require.Nil(t, err)
		return conn
	}
	f(getConn)
}

func getVTGateMysqlConn() *mysql.Conn {
	ctx := context.Background()

	params := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: KeyspaceName,
	}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		log.Fatal(err.Error())
	}
	return conn
}

func getBackendPrimaryMysqlConn() *mysql.Conn {
	ctx := context.Background()

	params := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().MySQLPort,
		DbName: KeyspaceName,
		Uname:  "vt_dba",
	}
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		log.Fatal(err.Error())
	}
	return conn
}
