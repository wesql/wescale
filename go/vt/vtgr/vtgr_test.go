package vtgr

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/wesql/wescale/go/sync2"
	"github.com/wesql/wescale/go/vt/topo/memorytopo"
	"github.com/wesql/wescale/go/vt/vtgr/config"
	"github.com/wesql/wescale/go/vt/vtgr/controller"
	"github.com/wesql/wescale/go/vt/vtgr/db"
	"github.com/wesql/wescale/go/vt/vttablet/tmclient"

	topodatapb "github.com/wesql/wescale/go/vt/proto/topodata"
)

func TestSighupHandle(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	vtgr := newVTGR(
		ctx,
		ts,
		tmclient.NewTabletManagerClient(),
	)
	var shards []*controller.GRShard
	config := &config.VTGRConfig{
		DisableReadOnlyProtection:   false,
		BootstrapGroupSize:          5,
		MinNumReplica:               3,
		BackoffErrorWaitTimeSeconds: 10,
		BootstrapWaitTimeSeconds:    10 * 60,
	}
	shards = append(shards, controller.NewGRShard("ks", "0", nil, vtgr.tmc, vtgr.topo, db.NewVTGRSqlAgent(), config, localDbPort, true))
	vtgr.Shards = shards
	shard := vtgr.Shards[0]
	shard.LockShard(ctx, "test")
	res := sync2.NewAtomicInt32(0)
	vtgr.handleSignal(func(i int) {
		res.Set(1)
	})
	assert.NotNil(t, shard.GetUnlock())
	assert.False(t, vtgr.stopped.Get())
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), res.Get())
	assert.Nil(t, shard.GetUnlock())
	assert.True(t, vtgr.stopped.Get())
}
