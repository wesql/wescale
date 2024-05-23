/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	topodatapb "github.com/wesql/wescale/go/vt/proto/topodata"
	"github.com/wesql/wescale/go/vt/topo"
	"github.com/wesql/wescale/go/vt/vtconsensus/db"
	"github.com/wesql/wescale/go/vt/vtconsensus/inst"
)

// TestWeSQLIssueDiagnose is a test for the wesql-server diagnose function
func TestWeSQLServerIssueDiagnose(t *testing.T) {
	// Make sure ping tablet timeout is short enough to not affect the test
	pingTabletTimeout = 5 * time.Second

	type tabletdata struct {
		alias           string
		uid             int
		localView       db.TestConsensusLocalView
		TabletMySQLHost string
		TabletMySQLPort int
		tabletType      topodatapb.TabletType
	}

	var sqlTests = []struct {
		name            string
		expected        DiagnoseType
		errMessage      string
		tabletData      []tabletdata
		consensusMember []db.TestConsensusMember
		removeTablets   []string // to simulate missing tablet in topology
	}{
		{name: "vitess and wesql-server all healthy", expected: DiagnoseTypeHealthy, tabletData: []tabletdata{
			{testAlias0, testUID0,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{alias: testAlias2, uid: testUID1,
				localView:       db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				TabletMySQLHost: testMySQLConsensusHost2, TabletMySQLPort: testMySQLPort,
				tabletType: topodatapb.TabletType_REPLICA,
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "no leader", expected: DiagnoseTypeMissingConsensusLeader, errMessage: "wesql-server no consensus leader available", tabletData: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, LeaderHostPort: testMySQLPort, Role: db.CANDIDATE},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{alias: testAlias1, uid: testUID1,
				localView:       db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, LeaderHostPort: testMySQLPort, Role: db.CANDIDATE},
				TabletMySQLHost: testMySQLConsensusHost1, TabletMySQLPort: testMySQLPort,
				tabletType: topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, LeaderHostPort: testMySQLPort, Role: db.CANDIDATE},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			consensusMember: []db.TestConsensusMember{}, removeTablets: []string{},
		},
		// wesql-server encountered a network error or other error when communicating with vtconsensus,
		// which resulted in vtconsensus being unable to communicate with the leader. In this case,
		// it may be possible to determine that the leader is actually available by using another instance.
		{"leader unreachable", DiagnoseTypeMissingConsensusLeader, "wesql-server no consensus leader available", []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			[]db.TestConsensusMember{}, []string{},
		},
		// When the leader is found through the local view and the global view is checked,
		// if wesql-server triggers a failover at this time, then the wesql_cluster_global will be empty.
		{"leader switchover during intermediate execution process", DiagnoseTypeMissingConsensusLeader, "wesql-server no consensus leader available", []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			[]db.TestConsensusMember{ // The global view is empty.
				{},
				{},
				{},
			}, []string{},
		},
		// Leader local view reading is normal, but a error occurred when attempting to read leader wesql_cluster_global.
		// For example, the leader is down or a network error occurred.
		{"leader unreachable during intermediate execution process", DiagnoseTypeUnreachableConsensusLeader, "wesql-server leader unreachable", []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			[]db.TestConsensusMember{}, []string{},
		},
		{name: "one follower unreachable", expected: DiagnoseTypeHealthy, tabletData: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{"two nodes unreachable", DiagnoseTypeMissingConsensusLeader, "wesql-server no consensus leader available", []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, LeaderHostPort: testMySQLPort, Role: db.CANDIDATE},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			[]db.TestConsensusMember{}, []string{},
		},
		{"all nodes unreachable", DiagnoseTypeMissingConsensusLeader, "wesql-server no consensus leader available", []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			[]db.TestConsensusMember{}, []string{},
		},
		// It is possible for consensus to have multi different leaders seen through local views . (such as during a master switch).
		// Then the leader with a larger term should be chosen as the leader.
		{name: "multi leaders", expected: DiagnoseTypeHealthy, tabletData: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 2, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 2, CurrentLeader: "localhost1:13306", LeaderHostName: testMySQLConsensusHost1, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER, IsRW: 1},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost1:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1},
			}, removeTablets: []string{},
		},
		{name: "leader and primary mismatch", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "vitess wrong primary tablet", tabletData: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				topodatapb.TabletType_PRIMARY,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				topodatapb.TabletType_REPLICA,
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
	}
	for _, tt := range sqlTests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			dbAgent := db.NewMockAgent(ctrl)

			ts := NewMockConsensusTopo(ctrl)
			tmc := NewMockConsensusTmcClient(ctrl)

			primaryTs := time.Now()

			tablets := make(map[string]*topo.TabletInfo)
			localViewData := make(map[string]db.TestConsensusLocalView)

			for _, td := range tt.tabletData {
				tablet := buildTabletInfoWithCell(uint32(td.uid), testCell, testKeyspace, testUnShard, td.TabletMySQLHost, int32(td.TabletMySQLPort), td.tabletType, primaryTs)
				tablets[td.alias] = tablet
				localViewData[td.alias] = td.localView

				dbAgent.
					EXPECT().
					FetchConsensusLocalView(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(alias string, instanceKey *inst.InstanceKey,
						globalView *db.ConsensusGlobalView) (*db.ConsensusLocalView, error) {
						return db.BuildConsensusLocalView(alias, instanceKey.Hostname, instanceKey.Port, localViewData[alias], globalView)
					}).
					AnyTimes()
			}
			dbAgent.
				EXPECT().
				NewConsensusGlobalView().
				Return(&db.ConsensusGlobalView{}).
				AnyTimes()

			dbAgent.
				EXPECT().
				FetchConsensusGlobalView(gomock.Any()).
				DoAndReturn(func(globalView *db.ConsensusGlobalView) error {
					return db.BuildConsensusGlobalView(tt.consensusMember, globalView)
				}).
				AnyTimes()
			ts.
				EXPECT().
				GetShard(gomock.Any(), gomock.Eq(testKeyspace), gomock.Eq(testUnShard)).
				Return(&topo.ShardInfo{Shard: &topodatapb.Shard{}}, nil)

			ts.
				EXPECT().
				GetTabletMapForShardByCell(gomock.Any(), gomock.Eq(testKeyspace), gomock.Eq(testUnShard), gomock.Any()).
				Return(tablets, nil)

			tmc.
				EXPECT().
				Ping(gomock.Any(), gomock.Any()).
				Return(nil).
				AnyTimes()
			ctx := context.Background()
			shard := NewConsensusShard(testKeyspace, testUnShard, []string{testCell}, tmc, ts, dbAgent, 0)
			shard.RefreshTabletsInShardWithLock(ctx)
			diagnose, err := shard.Diagnose(ctx)
			assert.Equal(t, tt.expected, diagnose)
			if tt.errMessage == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errMessage), err.Error())
			}
		})
	}
}

// TestPrimaryTabletIssueDiagnose is a test for the vitess primary tablet diagnose function
func TestPrimaryTabletIssueDiagnose(t *testing.T) {
	// Make sure ping tablet timeout is short enough to not affect the test
	pingTabletTimeout = 5 * time.Second
	primaryTs := time.Now()

	type tabletdata struct {
		alias            string
		uid              int
		localView        db.TestConsensusLocalView
		TabletMySQLHost  string
		TabletMySQLPort  int
		isOnline         bool
		ttype            topodatapb.TabletType
		primaryTimestamp time.Time
	}

	var sqltests = []struct {
		name            string
		expected        DiagnoseType
		errMessage      string
		ttdata          []tabletdata
		consensusMember []db.TestConsensusMember
		removeTablets   []string // to simulate missing tablet in topology
	}{
		{name: "vitess and wesql-server all healthy", expected: DiagnoseTypeHealthy, ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "primary tablet and wesql-server leader mismatch", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "vitess wrong primary tablet", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "no primary tablet", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "vitess wrong primary tablet", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "unreachable primary tablet", expected: DiagnoseTypeUnreachablePrimary, errMessage: "vitess primary tablet unreachable", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				false, topodatapb.TabletType_PRIMARY, // tablet no exists
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "unreachable wrong primary tablet", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "vitess wrong primary tablet", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_REPLICA, // tablet uninitialized
				time.Time{},
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				false, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "uninitialized primary tablet", expected: DiagnoseTypeMissingConsensusLeader, errMessage: "wesql-server no consensus leader available", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY, // tablet uninitialized by removeTablets
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{testAlias0}, // tablet uninitialized
		},
		{name: "non primary tablet unreachable", expected: DiagnoseTypeHealthy, ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				false, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				false, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "uninitialized non primary tablet", expected: DiagnoseTypeHealthy, ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{testAlias1, testAlias2},
		},
		{name: "uninitialized all tablet", expected: DiagnoseTypeMissingConsensusLeader, errMessage: "wesql-server no consensus leader available", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{testAlias0, testAlias1, testAlias2},
		},
		{name: "multi primary tablet,exist a newer primaryTimestamp", expected: DiagnoseTypeHealthy, errMessage: "", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs.Add(1 * time.Minute),
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
		{name: "multi primary tablet,and tablets primaryTimestamp are equal", expected: DiagnoseTypeHealthy, errMessage: "", ttdata: []tabletdata{
			{testAlias0, testUID1,
				db.TestConsensusLocalView{ServerID: 1, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.LEADER, IsRW: 1},
				testMySQLConsensusHost0, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias1, testUID1,
				db.TestConsensusLocalView{ServerID: 2, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost1, testMySQLPort,
				true, topodatapb.TabletType_PRIMARY,
				primaryTs,
			},
			{testAlias2, testUID1,
				db.TestConsensusLocalView{ServerID: 3, CurrentTerm: 1, CurrentLeader: "localhost0:13306", LeaderHostName: testMySQLConsensusHost0, LeaderHostPort: testMySQLPort, Role: db.FOLLOWER},
				testMySQLConsensusHost2, testMySQLPort,
				true, topodatapb.TabletType_REPLICA,
				time.Time{},
			},
		},
			consensusMember: []db.TestConsensusMember{
				{ServerID: 1, MySQLHost: testMySQLConsensusHost0, MySQLPort: testMySQLPort, Role: db.LEADER, ElectionWeight: 1, Connected: true},
				{ServerID: 2, MySQLHost: testMySQLConsensusHost1, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
				{ServerID: 3, MySQLHost: testMySQLConsensusHost2, MySQLPort: testMySQLPort, Role: db.FOLLOWER, ElectionWeight: 1, Connected: true},
			}, removeTablets: []string{},
		},
	}
	for _, tt := range sqltests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			dbAgent := db.NewMockAgent(ctrl)

			ts := NewMockConsensusTopo(ctrl)
			tmc := NewMockConsensusTmcClient(ctrl)

			tablets := make(map[string]*topo.TabletInfo)
			localViewData := make(map[string]db.TestConsensusLocalView)

			for _, td := range tt.ttdata {
				var response = struct {
					isOnline bool
				}{td.isOnline}
				tablet := buildTabletInfoWithCell(uint32(td.uid), testCell, testKeyspace, testUnShard, td.TabletMySQLHost, int32(td.TabletMySQLPort), td.ttype, td.primaryTimestamp)
				tablets[td.alias] = tablet

				localViewData[td.alias] = td.localView

				dbAgent.
					EXPECT().
					FetchConsensusLocalView(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(alias string, instanceKey *inst.InstanceKey,
						globalView *db.ConsensusGlobalView) (*db.ConsensusLocalView, error) {
						return db.BuildConsensusLocalView(alias, instanceKey.Hostname, instanceKey.Port, localViewData[alias], globalView)
					}).
					AnyTimes()
				tmc.
					EXPECT().
					Ping(gomock.Any(), &topodatapb.Tablet{
						Alias:                tablet.Alias,
						Hostname:             tablet.Hostname,
						Keyspace:             tablet.Keyspace,
						Shard:                tablet.Shard,
						Type:                 tablet.Type,
						Tags:                 tablet.Tags,
						MysqlHostname:        tablet.MysqlHostname,
						MysqlPort:            tablet.MysqlPort,
						PrimaryTermStartTime: tablet.PrimaryTermStartTime,
					}).
					DoAndReturn(func(_ context.Context, t *topodatapb.Tablet) error {
						if !response.isOnline {
							return errors.New("unreachable")
						}
						return nil
					}).
					AnyTimes()
			}
			dbAgent.
				EXPECT().
				NewConsensusGlobalView().
				Return(&db.ConsensusGlobalView{}).
				AnyTimes()

			dbAgent.
				EXPECT().
				FetchConsensusGlobalView(gomock.Any()).
				DoAndReturn(func(globalView *db.ConsensusGlobalView) error {
					return db.BuildConsensusGlobalView(tt.consensusMember, globalView)
				}).
				AnyTimes()
			ts.
				EXPECT().
				GetShard(gomock.Any(), gomock.Eq(testKeyspace), gomock.Eq(testUnShard)).
				Return(&topo.ShardInfo{Shard: &topodatapb.Shard{}}, nil)

			ts.
				EXPECT().
				GetTabletMapForShardByCell(gomock.Any(), gomock.Eq(testKeyspace), gomock.Eq(testUnShard), gomock.Any()).
				Return(tablets, nil)

			for _, tid := range tt.removeTablets {
				delete(tablets, tid)
			}

			ctx := context.Background()
			shard := NewConsensusShard(testKeyspace, testUnShard, []string{testCell}, tmc, ts, dbAgent, 0)
			shard.RefreshTabletsInShardWithLock(ctx)
			diagnose, err := shard.Diagnose(ctx)
			assert.Equal(t, tt.expected, diagnose)
			if tt.errMessage == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errMessage), err.Error())
			}
		})
	}
}
