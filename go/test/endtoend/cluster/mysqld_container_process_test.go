package cluster

import (
	"fmt"
	"strconv"
	"testing"
)

const (
	DefaultConsensusPot = 13306
)

func TestPullImage(t *testing.T) {
	res := pullImage()
	if res != nil {
		t.Error("please install docker first")
	}
	t.Log("docker is installed")
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
		clusterInfo += fmt.Sprintf(":%d", DefaultConsensusPot)
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

		tabletDir := "vt_" + strconv.Itoa(i)

		mounts := []string{
			"/Users/liuyongqiang/wesql-scale/config/apecloud_mycnf:/etc/mysql/conf.d",
			"/Users/liuyongqiang/wesql-scale/config/apecloud_local_scripts:/docker-entrypoint-initdb.d/",
			fmt.Sprintf("/tmp/%s:/mysql", tabletDir),
		}

		con := NewContainerProcess(name, nw, ipAddr, port, env, mounts...)

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
	cluster.Teardown()

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

	//env["MYSQL_ALLOW_EMPTY_PASSWORD"] = "1"
	//env["MYSQL_INIT_CONSENSUS_PORT"] = "13306"
	//env["CLUSTER_ID"] = "1"
	env["CLUSTER_INFO"] = "192.169.0.2:13306;192.168.0.3:13306;192.168.0.4:13306@1"

	tabletDir := "vt_1"

	id := 1
	containername := fmt.Sprintf("mysql-server%d", id)

	// map localhost:17001 to container 3306
	port := 17000 + id

	// assume in the project root directory
	mounts := []string{
		"/Users/liuyongqiang/wesql-scale/config/apecloud_mycnf:/etc/mysql/conf.d",
		"/Users/liuyongqiang/wesql-scale/config/apecloud_local_scripts:/docker-entrypoint-initdb.d/",
		fmt.Sprintf("/tmp/%s:/mysql", tabletDir),
	}

	con := NewContainerProcess(containername, cn.Name, "192.168.0.2", port, env, mounts...)
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
