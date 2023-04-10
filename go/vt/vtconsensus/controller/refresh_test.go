/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
)

var (
	testCell                = "test_cell"
	testKeyspace            = "test_ks"
	testUnShard             = "0"
	testMySQLConsensusHost0 = "localhost0"
	testMySQLConsensusHost1 = "localhost1"
	testMySQLConsensusHost2 = "localhost2"
	testMySQLHost0          = "localhost0"
	testMySQLHost1          = "localhost1"
	testMySQLHost2          = "localhost2"
	testMySQLPort           = 3306
	testUID0                = 0
	testUID1                = 1
	testUID2                = 2
	testAlias0              = "test_cell-0000000000"
	testAlias1              = "test_cell-0000000001"
	testAlias2              = "test_cell-0000000002"
)

// TestRefreshTabletsWithEmptyCells tests the RefreshTabletInShardWithLock function.
func TestRefreshTabletsWithEmptyCells(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer(testCell)
	defer ts.Close()
	_ = ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{})
	_ = ts.CreateShard(ctx, testKeyspace, testUnShard)
	primaryTs := time.Now()

	tablet1 := buildTabletInfoWithCell(uint32(testUID0), testCell, testKeyspace, testUnShard, testMySQLHost0, int32(testMySQLPort), topodatapb.TabletType_PRIMARY, primaryTs.Add(1*time.Minute))
	tablet2 := buildTabletInfoWithCell(uint32(testUID1), testCell, testKeyspace, testUnShard, testMySQLHost1, int32(testMySQLPort), topodatapb.TabletType_PRIMARY, primaryTs)
	tablet3 := buildTabletInfoWithCell(uint32(testUID2), testCell, testKeyspace, testUnShard, testMySQLHost2, int32(0), topodatapb.TabletType_REPLICA, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)
	// with empty cell
	shard := NewConsensusShard(testKeyspace, testUnShard, nil, nil, ts, nil, 0)
	// get tablet instance and primary tablet from local topo server
	shard.RefreshTabletsInShardWithLock(ctx)
	instances := shard.instances
	assert.Equal(t, testAlias0, shard.PrimaryAlias)
	assert.Equal(t, 3, len(instances))
	assert.Equal(t, testAlias0, instances[0].alias)
	assert.Equal(t, testAlias1, instances[1].alias)
	assert.Equal(t, testAlias2, instances[2].alias)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, instances[0].tablet.Type)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, instances[1].tablet.Type)
	assert.Equal(t, topodatapb.TabletType_REPLICA, instances[2].tablet.Type)
	assert.Equal(t, int32(testMySQLPort), instances[0].tablet.MysqlPort)
	assert.Equal(t, int32(testMySQLPort), instances[1].tablet.MysqlPort)
	assert.Equal(t, int32(0), instances[2].tablet.MysqlPort)
	assert.Equal(t, testMySQLHost0, instances[0].tablet.MysqlHostname)
	assert.Equal(t, testMySQLHost1, instances[1].tablet.MysqlHostname)
	assert.Equal(t, testMySQLHost2, instances[2].tablet.MysqlHostname)
}

// TestRefreshTabletsWithCell tests the RefreshTabletInShardWithLock function with cell.
func TestRefreshTabletsWithCell(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer(testCell)
	defer ts.Close()
	_ = ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{})
	_ = ts.CreateShard(ctx, testKeyspace, testUnShard)
	primaryTs := time.Now()

	tablet1 := buildTabletInfoWithCell(uint32(testUID0), testCell, testKeyspace, testUnShard, testMySQLHost0, int32(testMySQLPort), topodatapb.TabletType_PRIMARY, primaryTs)
	tablet2 := buildTabletInfoWithCell(uint32(testUID1), testCell, testKeyspace, testUnShard, testMySQLHost1, int32(testMySQLPort), topodatapb.TabletType_REPLICA, time.Time{})
	tablet3 := buildTabletInfoWithCell(uint32(testUID2), testCell, testKeyspace, testUnShard, testMySQLHost2, int32(0), topodatapb.TabletType_RDONLY, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)

	// with local cell
	shard := NewConsensusShard(testKeyspace, testUnShard, []string{testCell}, nil, ts, nil, 0)
	// get tablet instance and primary tablet from local topo server
	shard.RefreshTabletsInShardWithLock(ctx)
	instances := shard.instances
	assert.Equal(t, testAlias0, shard.PrimaryAlias)
	// TabletType_RDONLY is filtered out
	assert.Equal(t, 2, len(instances))
	assert.Equal(t, testAlias0, instances[0].alias)
	assert.Equal(t, testAlias1, instances[1].alias)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, instances[0].tablet.Type)
	assert.Equal(t, topodatapb.TabletType_REPLICA, instances[1].tablet.Type)
	assert.Equal(t, int32(testMySQLPort), instances[0].tablet.MysqlPort)
	assert.Equal(t, int32(testMySQLPort), instances[1].tablet.MysqlPort)
	assert.Equal(t, testMySQLHost0, instances[0].tablet.MysqlHostname)
	assert.Equal(t, testMySQLHost1, instances[1].tablet.MysqlHostname)
}

func TestRefreshPrimaryShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer(testCell)
	defer ts.Close()
	_ = ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{})
	_ = ts.CreateShard(ctx, testKeyspace, testUnShard)
	primaryTs := time.Now()

	tablet1 := buildTabletInfoWithCell(uint32(testUID0), testCell, testKeyspace, testUnShard, testMySQLHost0, int32(testMySQLPort), topodatapb.TabletType_PRIMARY, primaryTs)
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)

	// update primary tablet in global topo server
	_, _ = ts.UpdateShardFields(ctx, testKeyspace, testUnShard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: testCell, Uid: uint32(testUID0)}
		si.PrimaryTermStartTime = &vttime.Time{Seconds: int64(primaryTs.Second()), Nanoseconds: int32(primaryTs.Nanosecond())}
		return nil
	})

	// empty cell
	shard := NewConsensusShard(testKeyspace, testUnShard, []string{testCell}, nil, ts, nil, 0)
	// get primary tablet from global topo server
	primaryAlias, _ := shard.refreshPrimaryShard(ctx)
	assert.Equal(t, testAlias0, primaryAlias)
}

func TestLockRelease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer(testCell)
	defer ts.Close()
	_ = ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{})
	_ = ts.CreateShard(ctx, testKeyspace, testUnShard)
	shard := NewConsensusShard(testKeyspace, testUnShard, nil, nil, ts, nil, 0)
	ctx, err := shard.LockShard(ctx, "")
	assert.NoError(t, err)
	// make sure we get the lock
	err = shard.checkShardLocked(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, shard.unlock)
	shard.UnlockShard()
	assert.Nil(t, shard.unlock)
	err = shard.checkShardLocked(ctx)
	assert.EqualError(t, err, "lost topology lock; aborting: shard test_ks/0 is not locked (no lockInfo in map)")
}

func buildTabletInfoWithCell(id uint32, cell, ks, shard, host string, mysqlPort int32,
	ttype topodatapb.TabletType, primaryTermTime time.Time) *topo.TabletInfo {
	alias := &topodatapb.TabletAlias{Cell: cell, Uid: id}
	return &topo.TabletInfo{Tablet: &topodatapb.Tablet{
		Alias:                alias,
		Hostname:             host,
		MysqlHostname:        host,
		MysqlPort:            mysqlPort,
		Keyspace:             ks,
		Shard:                shard,
		Type:                 ttype,
		PrimaryTermStartTime: logutil.TimeToProto(primaryTermTime),
		Tags:                 map[string]string{"hostname": fmt.Sprintf("host_%d", id)},
	}}
}
