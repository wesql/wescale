/*
Copyright 2021 The Vitess Authors.

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

package engine

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

type failDBDDL struct{}

// CreateDatabase implements the DropCreateDB interface
func (failDBDDL) CreateDatabase(context.Context, string) error {
	return vterrors.VT12001("create database by failDBDDL")
}

// DropDatabase implements the DropCreateDB interface
func (failDBDDL) DropDatabase(context.Context, string) error {
	return vterrors.VT12001("drop database by failDBDDL")
}

type noOp struct{}

// CreateDatabase implements the DropCreateDB interface
func (noOp) CreateDatabase(context.Context, string) error {
	return nil
}

// DropDatabase implements the DropCreateDB interface
func (noOp) DropDatabase(context.Context, string) error {
	return nil
}

type apeCloudDbOp struct {
	srvTs srvtopo.Server
	gw    queryservice.QueryService
}

// RegisterApeCloudDbOp registers the apeCloudDbOp plugin
func RegisterApeCloudDbOp(srvTs srvtopo.Server, gateway queryservice.QueryService) {
	DBDDLRegister(apeCloudDbDDL, &apeCloudDbOp{srvTs: srvTs, gw: gateway})
}

// CreateDatabase implements the DropCreateDB interface
func (a apeCloudDbOp) CreateDatabase(ctx context.Context, keyspaceName string) error {
	ts, err := a.srvTs.GetTopoServer()
	if err != nil {
		return fmt.Errorf("GetTopoServer failed: %v", err)
	}
	cellName, err := ts.GetKnownCells(ctx)
	if err != nil {
		return fmt.Errorf("GetKnownCells failed: %v", err)
	}
	return topotools.CreateDatabase(ctx, ts, a.gw, keyspaceName, cellName)
}

// DropDatabase implements the DropCreateDB interface
func (a apeCloudDbOp) DropDatabase(ctx context.Context, keyspaceName string) error {
	ts, err := a.srvTs.GetTopoServer()
	if err != nil {
		return fmt.Errorf("GetTopoServer failed: %v", err)
	}
	cellName, err := ts.GetKnownCells(ctx)
	if err != nil {
		return fmt.Errorf("GetKnownCells failed: %v", err)
	}
	return topotools.DropDatabase(ctx, ts, keyspaceName, cellName)
}

const (
	faildbDDL          = "fail"
	noOpdbDDL          = "noop"
	apeCloudDbDDL      = "apeCloud"
	defaultDBDDLPlugin = apeCloudDbDDL
)

func init() {
	DBDDLRegister(faildbDDL, failDBDDL{})
	DBDDLRegister(noOpdbDDL, noOp{})
}
