package cluster

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const (
	DefaultCommand                = "docker"
	DefaultImageRepo              = "apecloud/apecloud-mysql-server"
	DefaultImageTag               = "latest"
	DefaultConfigMountDestination = "/etc/mysql/conf.d"
	DefaultScriptMountDestination = "/docker-entrypoint-initdb.d/"
	DefaultDataMountDestination   = "/mysql"
	DefaultPort                   = "3306"
	DefaultClusterInfo            = "CLUSTER_INFO"
	// todo
	DefaultDockerfile = ""
)

var (
	ImgRepo    string
	ImgTag     string
	Envs       map[string]string
	MountExist map[string]bool
)

func init() {
	ImgRepo = DefaultImageRepo
	ImgTag = DefaultImageTag
	repo, ok := os.LookupEnv("IMAGEREPO")
	if ok {
		ImgRepo = repo
	}
	tag, ok := os.LookupEnv("IMAGETAG")
	if ok {
		ImgTag = tag
	}

	Envs = make(map[string]string)

	// default environment variable
	Envs["MYSQL_ALLOW_EMPTY_PASSWORD"] = "1"
	Envs["MYSQL_INIT_CONSENSUS_PORT"] = "13306"
	Envs["CLUSTER_ID"] = "1"

	MountExist = make(map[string]bool)

	// default mounts
	MountExist[DefaultConfigMountDestination] = false
	MountExist[DefaultScriptMountDestination] = false
	MountExist[DefaultDataMountDestination] = false
}

type ContainerProcessCluster struct {
	ID         int
	Network    *ContainerNetwork
	Containers []*ContainerProcess
}

type ContainerProcess struct {
	Name    string
	Network string
	IPAddr  string
	Port    int
	Img     string
	ImgTag  string
	Mounts  []string
	Envs    map[string]string

	proc *exec.Cmd
	exit chan error
}

type ContainerProcessMounts struct {
	ConfigDir string
	ScriptDir string
	DataDir   string
}

type ContainerNetwork struct {
	Name    string
	Driver  string
	Gateway string
	Subnet  string
	proc    *exec.Cmd
}

func pullImage() error {
	if err := commandExist(); err != nil {
		log.Error(err)
		return err
	}

	image := fmt.Sprintf("%s:%s", ImgRepo, ImgTag)

	pull := exec.Command(DefaultCommand, "pull", image)

	if err := pull.Start(); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// todo build local img
func makeImage() error {
	if err := commandExist(); err != nil {
		log.Error(err)
		return err
	}
	// build img
	// modify ImgRepo ImgTag

	return nil
}

func commandExist() error {
	check := exec.Command(DefaultCommand, "--version")
	if err := check.Run(); err != nil {
		log.Info("docker doesn't exist")
		return fmt.Errorf("%s not exist", DefaultCommand)
	}
	return nil
}

func NewContainerProcess(name string, network string, ipaddr string, port int, envs map[string]string, mounts ...string) *ContainerProcess {
	// incorporate with default environment variable
	for env, value := range Envs {
		if _, ok := envs[env]; !ok {
			envs[env] = value
		}
	}

	// check if all  default mounts exist
	for i := range mounts {
		mount := strings.Split(mounts[i], ":")
		if val, ok := MountExist[mount[1]]; ok && !val {
			MountExist[mount[1]] = true
		}
	}

	for _, v := range MountExist {
		if !v {
			return nil
		}
	}

	return &ContainerProcess{
		Name:    name,
		Network: network,
		IPAddr:  ipaddr,
		Port:    port,
		Envs:    envs,
		Mounts:  mounts,
		Img:     ImgRepo,
		ImgTag:  ImgTag,
	}
}

func NewContainerNetWork(name string, driver string, gateway string, subnet string) *ContainerNetwork {
	return &ContainerNetwork{
		Name:    name,
		Driver:  driver,
		Gateway: gateway,
		Subnet:  subnet,
	}
}

func (cn *ContainerNetwork) Setup() error {

	cn.CheckAndRemove()

	log.Infof("creating container network %s", cn.Name)

	// create container network
	cn.proc = exec.Command(
		DefaultCommand,
		"network",
		"create",
		"--driver", cn.Driver,
		"--gateway", cn.Gateway,
		"--subnet", cn.Subnet,
		cn.Name,
	)
	err := cn.proc.Run()
	return err
}

func (cn *ContainerNetwork) CheckAndRemove() {
	// remove original container network with the same name as cn.Name
	exist := exec.Command(
		DefaultCommand,
		"network",
		"inspect",
		cn.Name,
	)

	if err := exist.Run(); err == nil {
		log.Infof("removing container network %s", cn.Name)
		_ = exec.Command(
			DefaultCommand,
			"network",
			"rm",
			cn.Name,
		).Run()
	}
}

func (cn *ContainerNetwork) ClearUp() {
	cn.CheckAndRemove()
}

func (container *ContainerProcess) Start() error {

	args := []string{
		"run",
		"--name", container.Name,
		"--network", container.Network,
		"--ip", container.IPAddr,
		"-p", fmt.Sprintf("%d:%s", container.Port, DefaultPort), // port mapping
	}

	// add mount mapping arg
	for _, mount := range container.Mounts {
		args = append(args, "-v")
		args = append(args, mount)
	}

	image := fmt.Sprintf("%s:%s", container.Img, container.ImgTag)

	// add environment variables
	for name, value := range container.Envs {

		args = append(args, "-e")

		if name == DefaultClusterInfo {
			value = "\"" + value + "\""
		}

		env := fmt.Sprintf("%s=%s", name, value)

		args = append(args, env)
	}

	args = append(args, image)

	tmp := strings.Join(args, " ")

	fmt.Println(tmp)

	container.proc = exec.Command(
		DefaultCommand,
		args...,
	)

	if err := container.proc.Start(); err != nil {
		log.Infof("start mysqld container %s failed", container.Name)
		return err
	}

	container.exit = make(chan error)
	go func() {
		if container.proc != nil {
			container.exit <- container.proc.Wait()
			log.Infof("vtconsensus exiting")
			close(container.exit)
		}
	}()

	return nil
}

func (container *ContainerProcess) Teardown() error {
	// container hasn't started
	if container.proc == nil || container.exit == nil {
		return nil
	}

	// Attempt gracefully shutdown with  SIGTERM
	_ = container.proc.Process.Signal(syscall.SIGTERM)

	// wait 10s for container to shut down
	select {
	case <-container.exit:
		container.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		// force to kill container process
		_ = container.proc.Process.Kill()
		err := <-container.exit
		container.proc = nil
		return err
	}
}

func (container *ContainerProcess) TeardownAndClearUp() error {

	if err := container.Teardown(); err != nil {
		log.Error(err)
		return err
	}

	clear := exec.Command(
		DefaultCommand,
		"container",
		"rm",
		container.Name,
	)

	if err := clear.Run(); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func NewContainerProcessCluster(id int, network *ContainerNetwork, containers ...*ContainerProcess) *ContainerProcessCluster {
	return &ContainerProcessCluster{
		ID:         id,
		Network:    network,
		Containers: containers,
	}
}

func (cluster *ContainerProcessCluster) CheckIntegrity() error {

	for _, con := range cluster.Containers {
		if con.Network != cluster.Network.Name {
			return fmt.Errorf("container %s does not be in container network %s", con.Name, cluster.Network.Name)
		}
	}

	var clusterInfo string
	clusterMember := make(map[int]bool)
	for i := 1; i <= len(cluster.Containers); i++ {
		clusterMember[i] = false
	}

	// check containers' cluster information
	for _, con := range cluster.Containers {
		clusterInfoWithID, ok := con.Envs[DefaultClusterInfo]
		if !ok {
			return fmt.Errorf("container %s does not have clusterinfo", con.Name)
		}

		cinfo := strings.Split(clusterInfoWithID, "@")

		if clusterInfo == "" {
			clusterInfo = cinfo[0]
		} else {
			if cinfo[0] != clusterInfo {
				return fmt.Errorf("container %s cluster info %s mismatched with cluster info %s", con.Name, con.Envs[DefaultClusterInfo], clusterInfo)
			}

			id, _ := strconv.Atoi(cinfo[1])

			if showUp, ok := clusterMember[id]; !ok {
				return fmt.Errorf("container %s id %d is beyond cluster container [1..%d]", con.Name, id, len(cluster.Containers))
			} else if showUp {
				return fmt.Errorf("container %s id show up more than once", con.Name)
			} else {
				clusterMember[id] = true
			}
		}
	}

	return nil
}

func (cluster *ContainerProcessCluster) Start() error {

	success := true

	for _, con := range cluster.Containers {
		if err := con.Start(); err != nil {
			log.Errorf("container %s start failed", con.Name)
			success = false
			break
		}
	}

	// if error occurs in start, shutdown all container
	if !success {
		for _, con := range cluster.Containers {
			if con.exit != nil {
				_ = con.Teardown()
			}
		}

		return errors.New("container cluster start failed")
	}

	return nil
}

func (cluster *ContainerProcessCluster) Teardown() {

	for _, con := range cluster.Containers {
		if err := con.Teardown(); err != nil {
			log.Errorf("container %s teardown failed")
		}
	}

}
