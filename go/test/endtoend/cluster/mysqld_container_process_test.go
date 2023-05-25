/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package cluster

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestPullImage(t *testing.T) {
	if err := pullImage(); err != nil {
		t.Error("pulling default docker image failed")
		return
	}
}

func TestContainerNetwork(t *testing.T) {
	nw := "my_wesqlscale_network"
	cn := NewContainerNetWork(nw, "bridge", "192.168.0.1", "192.168.0.0/24")

	if err := cn.Setup(); err != nil {
		t.Errorf("create container network %s failed", nw)
		return
	}

	t.Logf("container network %s has been created", nw)

	cn.ClearUp()
	t.Logf("container network %s has been cleared up", cn.Name)

}

func TestContainerProcessCluster(t *testing.T) {
	nw := "my_wesqlscale_network"
	cn := NewContainerNetWork(nw, "bridge", "192.168.0.1", "192.168.0.0/24")
	if err := cn.Setup(); err != nil {
		t.Errorf("create container network %s failed", nw)
		return
	}
	defer cn.ClearUp()

	// todo use getreversedport
	portBase := 17000

	var clusterInfo string
	// handle environment variable
	for i := 1; i <= 3; i++ {
		clusterInfo += "192.168.0."
		clusterInfo += strconv.Itoa(i + 1)
		clusterInfo += fmt.Sprintf(":%s", DefaultConsensusPot)
		clusterInfo += ";"
	}

	var containers []*ContainerProcess

	// create 3 container cluster
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("mysql-server%d", i)
		port := portBase + i
		ipAddr := "192.168.0." + strconv.Itoa(i+1)
		clusterInfoWithID := clusterInfo[:len(clusterInfo)-1] + "@" + strconv.Itoa(i)
		env := make(map[string]string)
		env["CLUSTER_INFO"] = clusterInfoWithID

		tabletDir := fmt.Sprintf("%s/vt_%d", os.Getenv("VTDATAROOT"), i)

		con := NewContainerProcess(name, nw, ipAddr, port, tabletDir, env)

		containers = append(containers, con)
	}

	cluster := NewContainerProcessCluster(1, cn, containers...)
	if err := cluster.CheckIntegrity(); err != nil {
		t.Error("container process cluster check integrity failed")
		return
	}
	if err := cluster.Start(); err != nil {
		t.Error(err)
		return
	}
	cluster.TeardownAndClearUp()
}

func TestContainerProcess_Start(t *testing.T) {
	nw := "my_wesqlscale_network"
	cn := NewContainerNetWork(nw, "bridge", "192.168.0.1", "192.168.0.0/24")
	if err := cn.Setup(); err != nil {
		t.Errorf("container network %s setup failed", nw)
		return
	}

	defer cn.ClearUp()

	env := make(map[string]string)

	env["CLUSTER_INFO"] = "192.169.0.2:13306;192.168.0.3:13306;192.168.0.4:13306@1"

	tabletDir := fmt.Sprintf("%s/vt_%d", os.Getenv("VTDATAROOT"), 1)

	id := 1
	containername := fmt.Sprintf("mysql-server%d", id)

	// map localhost:17001 to container 3306
	port := 17000 + id

	con := NewContainerProcess(containername, cn.Name, "192.168.0.2", port, tabletDir, env)
	if con == nil {
		t.Errorf("init container process %s failed", containername)
	}

	if err := con.Start(); err != nil {
		t.Error(err)
		return
	}
	t.Log("mysqld container has started...")
	defer con.TeardownAndClearUp()

}

func TestCommandExist(t *testing.T) {
	if err := commandExist(); err != nil {
		t.Error(err)
	}
}
