/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtconsensus

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sync2"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtconsensus/controller"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// Test handleSignal
func TestHandleSignal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	defer ts.Close()
	_ = ts.CreateKeyspace(ctx, "test_ks", &topodatapb.Keyspace{})
	_ = ts.CreateShard(ctx, "test_ks", "0")
	vtconsensus := newVTConsensus(
		ctx,
		ts,
		tmclient.NewTabletManagerClient(),
	)
	shard := controller.NewConsensusShard("test_ks", "0", nil, vtconsensus.tmc, vtconsensus.topo, db.NewVTConsensusSQLAgent(), localDbPort)
	vtconsensus.Shard = shard
	_, _ = shard.LockShard(ctx, "test")
	res := sync2.NewAtomicInt32(0)
	vtconsensus.handleSignal(func(i int) {
		res.Set(1)
	})
	assert.NotNil(t, shard.GetUnlock())
	assert.False(t, vtconsensus.stopped.Get())
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), res.Get())
	assert.Nil(t, shard.GetUnlock())
	assert.True(t, vtconsensus.stopped.Get())
}
