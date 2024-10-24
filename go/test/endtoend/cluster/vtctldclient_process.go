/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtctldClientProcess is a generic handle for a running vtctldclient command .
// It can be spawned manually
type VtctldClientProcess struct {
	Name          string
	Binary        string
	Server        string
	TempDirectory string
	ZoneName      string
}

// ExecuteCommand executes any vtctldclient command
func (vtctldclient *VtctldClientProcess) ExecuteCommand(args ...string) (err error) {
	output, err := vtctldclient.ExecuteCommandWithOutput(args...)
	if output != "" {
		if err != nil {
			log.Errorf("Output:\n%v", output)
		}
	}
	return err
}

// ExecuteCommandWithOutput executes any vtctldclient command and returns output
func (vtctldclient *VtctldClientProcess) ExecuteCommandWithOutput(args ...string) (string, error) {
	var resultByte []byte
	var resultStr string
	var err error
	retries := 10
	retryDelay := 1 * time.Second
	pArgs := []string{"--server", vtctldclient.Server}
	if *isCoverage {
		pArgs = append(pArgs, "--test.coverprofile="+getCoveragePath("vtctldclient-"+args[0]+".out"), "--test.v")
	}
	pArgs = append(pArgs, args...)
	for i := 1; i <= retries; i++ {
		tmpProcess := exec.Command(
			vtctldclient.Binary,
			//filterDoubleDashArgs(pArgs, vtctldclient.VtctldClientMajorVersion)...,
			pArgs...,
		)
		log.Infof("Executing vtctldclient with command: %v (attempt %d of %d)", strings.Join(tmpProcess.Args, " "), i, retries)
		resultByte, err = tmpProcess.CombinedOutput()
		resultStr = string(resultByte)
		if err == nil || !shouldRetry(resultStr) {
			break
		}
		time.Sleep(retryDelay)
	}
	return filterResultWhenRunsForCoverage(resultStr), err
}

// VtctldClientProcessInstance returns a VtctldProcess handle for vtctldclient process
// configured with the given Config.
func VtctldClientProcessInstance(hostname string, grpcPort int, tmpDirectory string) *VtctldClientProcess {
	vtctldclient := &VtctldClientProcess{
		Name:          "vtctldclient",
		Binary:        "vtctldclient",
		Server:        fmt.Sprintf("%s:%d", hostname, grpcPort),
		TempDirectory: tmpDirectory,
	}
	return vtctldclient
}
