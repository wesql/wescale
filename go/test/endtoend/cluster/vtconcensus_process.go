/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package cluster

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

type VtconsensusProcess struct {
	Name          string
	Binary        string
	DataDirectory string
	LogDirectory  string
	//ListenClientURL    string
	//AdvertiseClientURL string
	Port int
	Host string
	//VerifyURL          string
	//PeerURL            string
	//ZKPorts            string

	// topo server info
	TopoImpl             string
	TopoGlobalServerAddr string
	TopoGlobalRoot       string

	// vtconcensus
	ScanInterval      int
	RefreshInterval   int
	ScanRepairTimeOut int
	DBUserName        string
	DBPasswd          string

	// extraArgs
	ExtraArgs []string

	proc *exec.Cmd
	exit chan error
}

func (vtcon *VtconsensusProcess) Start() error {
	args := []string{
		"--topo_implementation", vtcon.TopoImpl,
		"--topo_global_server_address", vtcon.TopoGlobalServerAddr,
		"--topo_global_root", vtcon.TopoGlobalRoot,
		"--scan_interval", fmt.Sprintf("%ds", vtcon.ScanInterval),
		"--refresh_interval", fmt.Sprintf("%ds", vtcon.RefreshInterval),
		"--scan_repair_timeout", fmt.Sprintf("%ds", vtcon.ScanRepairTimeOut),
		"--log_dir", vtcon.LogDirectory,
		"--db_username", vtcon.DBUserName,
		fmt.Sprintf("--db_password=\"%s\"", vtcon.DBPasswd),
	}

	args = append(args, vtcon.ExtraArgs...)

	tmp := strings.Join(args, " ")
	fmt.Println(tmp)

	vtcon.proc = exec.Command(vtcon.Binary,
		args...,
	)

	err := vtcon.proc.Start()
	if err != nil {
		log.Infof("Start Vtconsensus with command %v failed", strings.Join(vtcon.proc.Args, " "))
		return err
	}

	vtcon.exit = make(chan error)
	go func() {
		if vtcon.proc != nil {
			vtcon.exit <- vtcon.proc.Wait()
			log.Infof("vtconsensus exiting")
			close(vtcon.exit)
		}
	}()

	return nil
}

func (vtcon *VtconsensusProcess) Teardown() error {
	// vtconsensus hasn't started
	if vtcon.proc == nil || vtcon.exit == nil {
		return nil
	}

	// Attempt gracefully shutdown with  SIGTERM
	_ = vtcon.proc.Process.Signal(syscall.SIGTERM)

	// wait 10s for vtconsensus to shut down
	select {
	case <-vtcon.exit:
		vtcon.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		// force to kill vtconsensus process
		_ = vtcon.proc.Process.Kill()
		err := <-vtcon.exit
		vtcon.proc = nil
		return err
	}
}

// VtconsensusInstance returns a VtConsensus process with the given configuration
func VtconsensusInstance(topoImpl string, topoGlobalServerAddr string, topoGlobalRoot string,
	scanInterval int, refreshInterval int,
	scanRepairTimeout int, logDir string,
	dbUserName string, dbPassWord string, extraArgs ...string) *VtconsensusProcess {
	return &VtconsensusProcess{
		Name:                 "vtconsensus",
		Binary:               "vtconsensus",
		TopoImpl:             topoImpl,
		TopoGlobalServerAddr: topoGlobalServerAddr,
		TopoGlobalRoot:       topoGlobalRoot,
		ScanInterval:         scanInterval,
		RefreshInterval:      refreshInterval,
		ScanRepairTimeOut:    scanRepairTimeout,
		LogDirectory:         logDir,
		DBUserName:           dbUserName,
		DBPasswd:             dbPassWord,
		ExtraArgs:            extraArgs,
	}
}
