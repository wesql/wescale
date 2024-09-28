/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"context"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestReadAfterWrite_TableLevel_Session(t *testing.T) {

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return
	}

	// Start keyspace
	Keyspace := &cluster.Keyspace{
		Name: KeyspaceName,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-transaction-timeout", "3", "--queryserver-config-max-result-size", "30"}
	if err := clusterInstance.StartUnshardedKeyspace(*Keyspace, 1, true); err != nil {
		log.Fatal(err.Error())
	}

	// Start vtgate
	clusterInstance.VtGateExtraArgs = []string{
		"--planner-version=gen4",
		"--warn_sharded_only=true",
	}
	if err := clusterInstance.StartTwoVtgate(); err != nil {
		log.Fatal(err.Error())
		return
	}

	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess
	if _, err := primaryTablet.QueryTablet("show databases", KeyspaceName, false); err != nil {
		log.Fatal(err.Error())
		return
	}

	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: KeyspaceName,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer conn.Close()
	_, err = conn.ExecuteFetch("create database if not exists "+DefaultKeyspaceName, 1000, false)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	runReadAfterWriteTest(t, true, "SESSION", false, false, false, false)
}
