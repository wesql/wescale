/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

var hcVTGateTest *discovery.FakeHealthCheck

var executeOptions = &querypb.ExecuteOptions{
	IncludedFields: querypb.ExecuteOptions_TYPE_ONLY,
}

var primarySession *vtgatepb.Session

func init() {
	createSandbox(KsTestDefaultShard).VSchema = `
{
	"sharded": false,
	"tables": {
		"t1": {}
	}
}
`
	hcVTGateTest = discovery.NewFakeHealthCheck(nil)
	transactionMode = "MULTI"
	Init(context.Background(), hcVTGateTest, newSandboxForCells([]string{"aa"}), "aa", nil, querypb.ExecuteOptions_Gen4)

	mysqlServerPort = 0
	mysqlAuthServerImpl = "none"
	initMySQLProtocol()
}

func TestVTGateExecute(t *testing.T) {
	createSandbox(KsTestDefaultShard)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestDefaultShard, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			Autocommit:   true,
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	want := *sandboxconn.SingleRowResult
	want.StatusFlags = 0 // VTGate result set does not contain status flags in sqltypes.Result
	utils.MustMatch(t, &want, qr)
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateExecuteWithKeyspaceShard(t *testing.T) {
	createSandbox(KsTestDefaultShard)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestDefaultShard, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)

	// Valid keyspace.
	_, qr, err := rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestDefaultShard,
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	wantQr := *sandboxconn.SingleRowResult
	wantQr.StatusFlags = 0 // VTGate result set does not contain status flags in sqltypes.Result
	utils.MustMatch(t, &wantQr, qr)

	// Invalid keyspace.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: "invalid_keyspace",
		},
		"select id from none",
		nil,
	)
	want := "VT05003: unknown database 'invalid_keyspace' in vschema"
	assert.EqualError(t, err, want)

	// Valid keyspace/shard.
	_, qr, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestDefaultShard + ":0@primary",
		},
		"select id from none",
		nil,
	)
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	utils.MustMatch(t, &wantQr, qr)

	// ignore invalid shard.
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: KsTestDefaultShard + ":noshard@primary",
		},
		"select id from none",
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), `no healthy tablet available for 'keyspace:"_vt" shard:"noshard" tablet_type:PRIMARY`)
}

func TestVTGateStreamExecute(t *testing.T) {
	ks := KsTestDefaultShard
	shard := "0"
	createSandbox(ks)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, ks, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
	var qrs []*sqltypes.Result
	err := rpcVTGate.StreamExecute(
		context.Background(),
		&vtgatepb.Session{
			TargetString: "@primary",
			Options:      executeOptions,
		},
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			qrs = append(qrs, r)
			return nil
		},
	)
	require.NoError(t, err)
	want := []*sqltypes.Result{{
		Fields: sandboxconn.StreamRowResult.Fields,
	}, {
		Rows: sandboxconn.StreamRowResult.Rows,
	}}
	utils.MustMatch(t, want, qrs)
	if !proto.Equal(sbc.Options[0], executeOptions) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], executeOptions)
	}
}

func TestVTGateBindVarError(t *testing.T) {
	ks := KsTestDefaultShard
	createSandbox(ks)
	hcVTGateTest.Reset()
	ctx := context.Background()
	session := &vtgatepb.Session{}
	bindVars := map[string]*querypb.BindVariable{
		"v": {
			Type:  querypb.Type_EXPRESSION,
			Value: []byte("1"),
		},
	}
	want := "v: invalid type specified for MakeValue: EXPRESSION"

	tcases := []struct {
		name string
		f    func() error
	}{{
		name: "Execute",
		f: func() error {
			_, _, err := rpcVTGate.Execute(ctx, session, "", bindVars)
			return err
		},
	}, {
		name: "ExecuteBatch",
		f: func() error {
			_, _, err := rpcVTGate.ExecuteBatch(ctx, session, []string{""}, []map[string]*querypb.BindVariable{bindVars})
			return err
		},
	}, {
		name: "StreamExecute",
		f: func() error {
			return rpcVTGate.StreamExecute(ctx, session, "", bindVars, func(_ *sqltypes.Result) error { return nil })
		},
	}}
	for _, tcase := range tcases {
		if err := tcase.f(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v error: %v, must contain %s", tcase.name, err, want)
		}
	}
}

func testErrorPropagation(t *testing.T, sbcs []*sandboxconn.SandboxConn, before func(sbc *sandboxconn.SandboxConn), after func(sbc *sandboxconn.SandboxConn), expected vtrpcpb.Code) {

	// Execute
	for _, sbc := range sbcs {
		before(sbc)
	}
	_, _, err := rpcVTGate.Execute(
		context.Background(),
		primarySession,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Errorf("error %v not propagated for Execute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got code %v err %v, want %v", ec, err, expected)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}

	// StreamExecute
	for _, sbc := range sbcs {
		before(sbc)
	}
	err = rpcVTGate.StreamExecute(
		context.Background(),
		primarySession,
		"select id from t1",
		nil,
		func(r *sqltypes.Result) error {
			return nil
		},
	)
	if err == nil {
		t.Errorf("error %v not propagated for StreamExecute", expected)
	} else {
		ec := vterrors.Code(err)
		if ec != expected {
			t.Errorf("unexpected error, got %v want %v: %v", ec, expected, err)
		}
	}
	for _, sbc := range sbcs {
		after(sbc)
	}
}

// TestErrorPropagation tests an error returned by sandboxconn is
// properly propagated through vtgate layers.  We need both a primary
// tablet and a rdonly tablet because we don't control the routing of
// Commit.
func TestErrorPropagation(t *testing.T) {
	createSandbox(KsTestDefaultShard)
	hcVTGateTest.Reset()
	// create a new session each time so that ShardSessions don't get re-used across tests
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}

	sbcm := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestDefaultShard, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbcrdonly := hcVTGateTest.AddTestTablet("aa", "1.1.1.2", 1001, KsTestDefaultShard, "0", topodatapb.TabletType_RDONLY, true, 1, nil)
	sbcs := []*sandboxconn.SandboxConn{
		sbcm,
		sbcrdonly,
	}

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_CANCELED] = 0
	}, vtrpcpb.Code_CANCELED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNKNOWN] = 0
	}, vtrpcpb.Code_UNKNOWN)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 0
	}, vtrpcpb.Code_INVALID_ARGUMENT)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_DEADLINE_EXCEEDED] = 0
	}, vtrpcpb.Code_DEADLINE_EXCEEDED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
	}, vtrpcpb.Code_ALREADY_EXISTS)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_PERMISSION_DENIED] = 0
	}, vtrpcpb.Code_PERMISSION_DENIED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0
	}, vtrpcpb.Code_RESOURCE_EXHAUSTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 0
	}, vtrpcpb.Code_FAILED_PRECONDITION)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0
	}, vtrpcpb.Code_ABORTED)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 0
	}, vtrpcpb.Code_INTERNAL)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAVAILABLE] = 0
	}, vtrpcpb.Code_UNAVAILABLE)

	testErrorPropagation(t, sbcs, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 20
	}, func(sbc *sandboxconn.SandboxConn) {
		sbc.MustFailCodes[vtrpcpb.Code_UNAUTHENTICATED] = 0
	}, vtrpcpb.Code_UNAUTHENTICATED)
}

// This test makes sure that if we start a transaction and hit a critical
// error, a rollback is issued.
func TestErrorIssuesRollback(t *testing.T) {
	createSandbox(KsTestDefaultShard)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestDefaultShard, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.Code_ABORTED case.
	session, _, err := rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{},
		"begin",
		nil,
	)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 20
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
	sbc.RollbackCount.Set(0)
	sbc.MustFailCodes[vtrpcpb.Code_ABORTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should trigger a rollback:
	// vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED case.
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{},
		"begin",
		nil,
	)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 20
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount.Get())
	}
	sbc.RollbackCount.Set(0)
	sbc.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 0

	// Start a transaction, send one statement.
	// Simulate an error that should *not* trigger a rollback:
	// vtrpcpb.Code_ALREADY_EXISTS case.
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		&vtgatepb.Session{},
		"begin",
		nil,
	)
	if err != nil {
		t.Fatalf("cannot start a transaction: %v", err)
	}
	session, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 20
	_, _, err = rpcVTGate.Execute(
		context.Background(),
		session,
		"select id from t1",
		nil,
	)
	if err == nil {
		t.Fatalf("want error but got nil")
	}
	if sbc.RollbackCount.Get() != 0 {
		t.Errorf("want 0, got %d", sbc.RollbackCount.Get())
	}
	sbc.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 0
}

func TestSetDefaultReadWriteSplittingPolicy(t *testing.T) {
	type args struct {
		strategy string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid strategy",
			args: args{
				strategy: "random",
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid strategy",
			args: args{
				strategy: "disable",
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid strategy",
			args: args{
				strategy: "foobar",
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid strategy",
			args: args{
				strategy: "",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, SetDefaultReadWriteSplittingPolicy(tt.args.strategy), fmt.Sprintf("SetDefaultReadWriteSplittingPolicy(%v)", tt.args.strategy))
		})
	}
}

func TestSetDefaultReadAfterWriteTimeout1(t *testing.T) {
	type args struct {
		timeoutStr string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid timeout",
			args: args{
				timeoutStr: "-1.0",
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid timeout",
			args: args{
				timeoutStr: "0",
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid timeout",
			args: args{
				timeoutStr: "89.5",
			},
			wantErr: assert.NoError,
		},
		{
			name: "valid timeout",
			args: args{
				timeoutStr: "30",
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid timeout",
			args: args{
				timeoutStr: "foobar",
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid timeout",
			args: args{
				timeoutStr: "",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, SetDefaultReadAfterWriteTimeout(tt.args.timeoutStr), fmt.Sprintf("SetDefaultReadAfterWriteTimeout(%v)", tt.args.timeoutStr))
		})
	}
}
