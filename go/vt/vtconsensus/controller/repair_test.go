/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
)

func TestWrongPrimaryTabletRepair(t *testing.T) {
	primaryTs := time.Now()

	type tabletData struct {
		alias            string
		uid              int
		ttType           topodatapb.TabletType
		tabletMySQLHost  string
		tabletMySQLPort  int
		uninitialized    bool
		primaryTimestamp time.Time
	}
	var testcases = []struct {
		name            string
		errorMsg        string
		LeaderMySQLHost string
		LeaderMySQLPort int
		inputs          []tabletData
	}{
		{
			name: "primary tablet and leader match", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "primary tablet and leader mismatch", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "no primary tablet", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "uninitialized primary tablet", errorMsg: "vitess no primary tablet available", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_UNKNOWN, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: true, primaryTimestamp: time.Time{}},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "uninitialized non primary tablet", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_UNKNOWN, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: true, primaryTimestamp: time.Time{}},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_UNKNOWN, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: true, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "multi primary tablet, exist a newer primaryTimestamp", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs.Add(1 * time.Minute)},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
		{
			name: "multi primary tablet, primaryTimestamp are equal", errorMsg: "", LeaderMySQLHost: testMySQLHost0, LeaderMySQLPort: testMySQLPort, inputs: []tabletData{
				{alias: testAlias0, uid: testUID0, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost0, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias1, uid: testUID1, ttType: topodatapb.TabletType_PRIMARY, tabletMySQLHost: testMySQLHost1, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: primaryTs},
				{alias: testAlias2, uid: testUID2, ttType: topodatapb.TabletType_REPLICA, tabletMySQLHost: testMySQLHost2, tabletMySQLPort: testMySQLPort, uninitialized: false, primaryTimestamp: time.Time{}},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Running test case: %s", tc.name)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			ts := memorytopo.NewServer(testCell)
			defer ts.Close()
			_ = ts.CreateKeyspace(ctx, testKeyspace, &topodatapb.Keyspace{})
			_ = ts.CreateShard(ctx, testKeyspace, testUnShard)

			dbAgent := db.NewMockAgent(ctrl)
			tmc := NewMockConsensusTmcClient(ctrl)

			tablets := make(map[string]*topo.TabletInfo)

			for _, input := range tc.inputs {
				if input.uninitialized == false {
					tablet := buildTabletInfoWithCell(uint32(input.uid), testCell, testKeyspace, testUnShard,
						input.tabletMySQLHost, int32(input.tabletMySQLPort), input.ttType, input.primaryTimestamp)
					tablets[input.alias] = tablet
					testutil.AddTablet(ctx, t, ts, tablet.Tablet, nil)
				}
			}

			dbAgent.
				EXPECT().
				NewConsensusGlobalView().
				Return(&db.ConsensusGlobalView{})

			tmc.
				EXPECT().
				ChangeType(gomock.Any(), gomock.Any(), topodatapb.TabletType_PRIMARY, false).
				Return(nil).
				AnyTimes()

			shard := NewConsensusShard(testKeyspace, testUnShard, []string{testCell}, tmc, ts, dbAgent, 0)
			populateGlobalViewForConsensusView(shard, tc.LeaderMySQLHost, tc.LeaderMySQLPort)
			shard.RefreshTabletsInShardWithLock(ctx)
			_, err := shard.Repair(ctx, DiagnoseTypeWrongPrimaryTablet)
			if tc.errorMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tc.errorMsg), err.Error())
			}
		})
	}
}

func populateGlobalViewForConsensusView(shard *ConsensusShard, host string, port int) {
	gv := shard.dbAgent.NewConsensusGlobalView()
	gv.LeaderTabletMySQLHost = host
	gv.LeaderTabletMySQLPort = port
	shard.sqlConsensusView.recordView(gv)
}
