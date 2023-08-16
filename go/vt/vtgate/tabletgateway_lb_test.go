/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestTabletGateway_PickTablet(t *testing.T) {
	type args struct {
		availableTablets []*discovery.TabletHealth
		options          *querypb.ExecuteOptions
	}
	tests := []struct {
		name         string
		lastSeenGtid *LastSeenGtid
		args         args
		want         []*discovery.TabletHealth
		wantErr      bool
	}{
		{
			name: "no availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: make([]*discovery.TabletHealth, 0),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "no options",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "",
				},
			},
			want: generateTabletHealthFromPosition(
				"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
			wantErr: false,
		},
		{
			name: "get all availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealthFromPosition(
				"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
			wantErr: false,
		},
		{
			name: "get partial availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want:    generateTabletHealthFromPosition("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &TabletGateway{
				localCell:    "test_cell",
				lastSeenGtid: tt.lastSeenGtid,
			}
			got, err := gw.PickTablet(tt.args.availableTablets, tt.args.options)
			if tt.wantErr {
				assert.EqualError(t, err, vterrors.VT14002().Error())
				return
			}
			assert.Contains(t, tt.want, got, "TestTabletGateway_PickTablet(%v, %v)", tt.args.availableTablets, tt.args.options)
		})
	}
}

func TestTabletGateway_filterAdvisorByGTIDThreshold(t *testing.T) {
	type args struct {
		availableTablets []*discovery.TabletHealth
		options          *querypb.ExecuteOptions
	}
	tests := []struct {
		name         string
		lastSeenGtid *LastSeenGtid
		args         args
		want         []*discovery.TabletHealth
	}{
		{
			name: "no availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: make([]*discovery.TabletHealth, 0),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "",
				},
			},
			want: make([]*discovery.TabletHealth, 0),
		},
		{
			name: "no options",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "",
				},
			},
			want: make([]*discovery.TabletHealth, 0),
		},
		{
			name: "get all availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealthFromPosition(
				"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
		},
		{
			name: "get partial availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealthFromPosition(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealthFromPosition("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &TabletGateway{
				lastSeenGtid: tt.lastSeenGtid,
			}
			assert.Equalf(t, tt.want, gw.filterAdvisorByGTIDThreshold(tt.args.availableTablets, tt.args.options), "filterAdvisorByGTIDThreshold(%v, %v)", tt.args.availableTablets, tt.args.options)
		})
	}
}

func generateTabletHealthFromPosition(positions ...string) []*discovery.TabletHealth {
	tabletHealths := make([]*discovery.TabletHealth, 0)
	for _, position := range positions {
		tabletHealths = append(tabletHealths, &discovery.TabletHealth{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "test_cell",
				},
			},
			Position: mysql.MustParsePosition(mysql.Mysql56FlavorID, position),
		})
	}
	return tabletHealths
}

func generateTabletHealthFromQPS(qpsList []float64, diffCellQPSList []float64) []*discovery.TabletHealth {
	tabletHealths := make([]*discovery.TabletHealth, 0)
	for _, qps := range qpsList {
		tabletHealths = append(tabletHealths, &discovery.TabletHealth{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "test_cell",
				},
			},
			Target: &querypb.Target{
				Cell: "test_cell",
			},
			Stats: &querypb.RealtimeStats{
				Qps: qps,
			},
		})
	}

	for _, qps := range diffCellQPSList {
		tabletHealths = append(tabletHealths, &discovery.TabletHealth{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "test_cell2",
				},
			},
			Target: &querypb.Target{
				Cell: "test_cell2",
			},
			Stats: &querypb.RealtimeStats{
				Qps: qps,
			},
		})
	}
	return tabletHealths
}

func TestTabletGateway_leastGlobalQpsLoadBalancer(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		wantQPS    float64
	}{
		{
			name:       "no candidates",
			candidates: generateTabletHealthFromQPS([]float64{}, []float64{}),
			wantQPS:    -1,
		},
		{
			name:       "1",
			candidates: generateTabletHealthFromQPS([]float64{1}, []float64{}),
			wantQPS:    1,
		},
		{
			name:       "412.3, 500.3, 600.3, 700.1, 8654.5, 2.1",
			candidates: generateTabletHealthFromQPS([]float64{412.3, 500.3, 600.3, 700.1, 8654.5, 2.1}, []float64{}),
			wantQPS:    2.1,
		},
		{
			name:       "412.3, 500.3, 600.3, 700.1, 8654.5 | 2.1",
			candidates: generateTabletHealthFromQPS([]float64{412.3, 500.3, 600.3, 700.1, 8654.5}, []float64{2.1}),
			wantQPS:    412.3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw := &TabletGateway{
				localCell: "test_cell",
			}
			chosen := gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_GLOBAL_QPS})
			if chosen == nil {
				assert.Equal(t, tt.wantQPS, -1.0)
				return
			}
			assert.Equalf(t, tt.wantQPS, chosen.Stats.Qps, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantQPS)
		})
	}
}

type tabletInfo struct {
	uid                uint32
	qps                float64
	cell               string
	position           string
	dbThreadsConnected int64
	dbThreadsRunning   int64
	tabletThreadsInUse int64
}

func genTablets(tabletInfoList []tabletInfo) []*discovery.TabletHealth {
	tabletHealths := make([]*discovery.TabletHealth, 0)
	for _, t := range tabletInfoList {
		tabletHealths = append(tabletHealths, &discovery.TabletHealth{
			Tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Uid:  t.uid,
					Cell: t.cell,
				},
			},
			Target: &querypb.Target{
				Cell: t.cell,
			},
			Stats: &querypb.RealtimeStats{
				Qps:                t.qps,
				MysqlThreadStats:   &querypb.MysqlThreadsStats{Connected: t.dbThreadsConnected, Running: t.dbThreadsRunning},
				TabletThreadsStats: t.tabletThreadsInUse,
			},
			Position: mysql.MustParsePosition(mysql.Mysql56FlavorID, t.position),
		})
	}

	return tabletHealths
}

type aggrInfo struct {
	tabletInfo
	queryCountInMinute uint64
	latencyInMinute    time.Duration
}

func genAggr(aggrInfo []aggrInfo) map[string]*TabletStatusAggregator {
	aggr := make(map[string]*TabletStatusAggregator)
	for _, a := range aggrInfo {
		name := fmt.Sprintf("%v", a.uid)
		aggr[name] = &TabletStatusAggregator{
			Name:               name,
			queryCountInMinute: [60]uint64{a.queryCountInMinute},
			latencyInMinute:    [60]time.Duration{a.latencyInMinute},
		}
	}
	return aggr
}

func TestTabletGateway_leastQpsLoadBalancer(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{}),
			},
			wantUid: 0,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell"},
			}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell"}, queryCountInMinute: 100 * 60},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell"}, queryCountInMinute: 200 * 60},
				}),
			},
			wantUid: 1,
		},
		{
			name: "500 400 300 | 100 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell2"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell2"},
			}),
			gw: &TabletGateway{
				localCell: "test_cell",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60},
				}),
			},
			wantUid: 3,
		},
		{
			name: "500 400 300 | 100 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell2"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell2"},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60},
				}),
			},
			wantUid: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_QPS})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

func TestTabletGateway_leastRTLoadBalancer(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{}),
			},
			wantUid: 0,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell"},
			}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 5,
		},
		{
			name: "500 400 300 | 100 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell2"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell2"},
			}),
			gw: &TabletGateway{
				localCell: "test_cell",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 5,
		},
		{
			name: "500 400 300 | 100 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell"},
				{uid: 4, cell: "test_cell"},
				{uid: 1, cell: "test_cell2"},
				{uid: 3, cell: "test_cell"},
				{uid: 2, cell: "test_cell2"},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_RT})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

func TestTabletGateway_leastMysqlConnectedConnections(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{}),
			},
			wantUid: 0,
		},
		{
			name: "all tablets in local cell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsConnected: 9},
				{uid: 4, cell: "test_cell", dbThreadsConnected: 7},
				{uid: 1, cell: "test_cell", dbThreadsConnected: 3},
				{uid: 3, cell: "test_cell", dbThreadsConnected: 4},
				{uid: 2, cell: "test_cell", dbThreadsConnected: 5},
			}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 1,
		},
		{
			name: "tablet with least connected connections not in local cell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsConnected: 5},
				{uid: 4, cell: "test_cell", dbThreadsConnected: 3},
				{uid: 1, cell: "test_cell2", dbThreadsConnected: 2},
				{uid: 3, cell: "test_cell", dbThreadsConnected: 4},
				{uid: 2, cell: "test_cell2", dbThreadsConnected: 3},
			}),
			gw: &TabletGateway{
				localCell: "test_cell",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 4,
		},
		{
			name: "tablets with least connected connections in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsConnected: 1000},
				{uid: 4, cell: "test_cell", dbThreadsConnected: 10004},
				{uid: 1, cell: "test_cell2", dbThreadsConnected: 1413},
				{uid: 3, cell: "test_cell", dbThreadsConnected: 4441313},
				{uid: 2, cell: "test_cell2", dbThreadsConnected: 424},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_MYSQL_CONNECTED_CONNECTIONS})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

func TestTabletGateway_leastMysqlRunningConnections(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{}),
			},
			wantUid: 0,
		},
		{
			name: "all tablets in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsRunning: 9},
				{uid: 4, cell: "test_cell", dbThreadsRunning: 7},
				{uid: 1, cell: "test_cell", dbThreadsRunning: 3},
				{uid: 3, cell: "test_cell", dbThreadsRunning: 4},
				{uid: 2, cell: "test_cell", dbThreadsRunning: 5},
			}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 1,
		},
		{
			name: "tablets with least running connections not in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsRunning: 5},
				{uid: 4, cell: "test_cell", dbThreadsRunning: 3},
				{uid: 1, cell: "test_cell2", dbThreadsRunning: 2},
				{uid: 3, cell: "test_cell", dbThreadsRunning: 4},
				{uid: 2, cell: "test_cell2", dbThreadsRunning: 3},
			}),
			gw: &TabletGateway{
				localCell: "test_cell",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 4,
		},
		{
			name: "tablet with least running connections in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsRunning: 1000},
				{uid: 4, cell: "test_cell", dbThreadsRunning: 10004},
				{uid: 1, cell: "test_cell2", dbThreadsRunning: 1413},
				{uid: 3, cell: "test_cell", dbThreadsRunning: 4441313},
				{uid: 2, cell: "test_cell2", dbThreadsRunning: 424},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 2,
		},
		{
			name: "all tablets not in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", dbThreadsRunning: 9223372036854775807},
				{uid: 4, cell: "test_cell", dbThreadsRunning: 922337203685477580},
				{uid: 1, cell: "test_cell2", dbThreadsRunning: 922337203685477},
				{uid: 3, cell: "test_cell", dbThreadsRunning: 92233720368547},
				{uid: 2, cell: "test_cell2", dbThreadsRunning: 9223372036854775806},
			}),
			gw: &TabletGateway{
				localCell: "test_cell3",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_MYSQL_RUNNING_CONNECTIONS})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

func TestTabletGateway_leastTabletInUseConnections(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{}),
			},
			wantUid: 0,
		},
		{
			name: "candidates in the same cell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", tabletThreadsInUse: 9},
				{uid: 4, cell: "test_cell", tabletThreadsInUse: 7},
				{uid: 1, cell: "test_cell", tabletThreadsInUse: 3},
				{uid: 3, cell: "test_cell", tabletThreadsInUse: 4},
				{uid: 2, cell: "test_cell", tabletThreadsInUse: 5},
				{uid: 6, cell: "test_cell", tabletThreadsInUse: 4},
				{uid: 7, cell: "test_cell", tabletThreadsInUse: 4},
				{uid: 8, cell: "test_cell", tabletThreadsInUse: 5},
			}),
			gw: &TabletGateway{
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 1,
		},
		{
			name: "tablet with the least connections not in local cell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", tabletThreadsInUse: 5},
				{uid: 4, cell: "test_cell", tabletThreadsInUse: 3},
				{uid: 1, cell: "test_cell2", tabletThreadsInUse: 2},
				{uid: 3, cell: "test_cell", tabletThreadsInUse: 4},
				{uid: 2, cell: "test_cell2", tabletThreadsInUse: 3},
			}),
			gw: &TabletGateway{
				localCell: "test_cell",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 4,
		},
		{
			name: "tablets with the least connections in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", tabletThreadsInUse: 1000},
				{uid: 4, cell: "test_cell", tabletThreadsInUse: 10004},
				{uid: 1, cell: "test_cell2", tabletThreadsInUse: 1413},
				{uid: 3, cell: "test_cell", tabletThreadsInUse: 4441313},
				{uid: 2, cell: "test_cell2", tabletThreadsInUse: 424},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 2,
		},
		{
			name: "extremely high number of connections",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", tabletThreadsInUse: 9223372036854775807},
				{uid: 4, cell: "test_cell", tabletThreadsInUse: 922337203685477580},
				{uid: 1, cell: "test_cell2", tabletThreadsInUse: 922337203685477},
				{uid: 3, cell: "test_cell", tabletThreadsInUse: 9223372036854775807},
				{uid: 2, cell: "test_cell2", tabletThreadsInUse: 9223372036854775806},
			}),
			gw: &TabletGateway{
				localCell: "test_cell2",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 1,
		},
		{
			name: "all tablets not in localcell",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", tabletThreadsInUse: 9223372036854775807},
				{uid: 4, cell: "test_cell", tabletThreadsInUse: 922337203685477580},
				{uid: 1, cell: "test_cell2", tabletThreadsInUse: 922337203685477},
				{uid: 3, cell: "test_cell", tabletThreadsInUse: 92233720368547},
				{uid: 2, cell: "test_cell2", tabletThreadsInUse: 9223372036854775806},
			}),
			gw: &TabletGateway{
				localCell: "test_cell3",
				statusAggregators: genAggr([]aggrInfo{
					{tabletInfo: tabletInfo{uid: 5, cell: "test_cell"}, queryCountInMinute: 500 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 4, cell: "test_cell"}, queryCountInMinute: 400 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 1, cell: "test_cell2"}, queryCountInMinute: 100 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 3, cell: "test_cell"}, queryCountInMinute: 300 * 60, latencyInMinute: 100 * time.Second},
					{tabletInfo: tabletInfo{uid: 2, cell: "test_cell2"}, queryCountInMinute: 200 * 60, latencyInMinute: 100 * time.Second},
				}),
			},
			wantUid: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_TABLET_INUSE_CONNECTIONS})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

func TestTabletGateway_leastBehindPrimaryLoadBalancer(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name:       "no candidates",
			candidates: genTablets([]tabletInfo{}),
			gw:         &TabletGateway{localCell: "test_cell"},
			wantUid:    0,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-500"},
				{uid: 4, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"},
				{uid: 3, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-300"},
				{uid: 2, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell"},
			wantUid: 5,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-500"},
				{uid: 4, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"},
				{uid: 3, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-300"},
				{uid: 2, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell"},
			wantUid: 3,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:500"},
				{uid: 4, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:100"},
				{uid: 3, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:300"},
				{uid: 2, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell2"},
			wantUid: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, &querypb.ExecuteOptions{LoadBalancePolicy: querypb.ExecuteOptions_LEAST_BEHIND_PRIMARY})
			if chosen == nil {
				assert.Equal(t, tt.wantUid, uint32(0))
				return
			}
			assert.Equalf(t, tt.wantUid, chosen.Tablet.Alias.Uid, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantUid)
		})
	}
}

// TestTabletGateway_loadBalance_options_is_nil tests the case that options is nil.
// In this case, the default load balance policy is RANDOM.
func TestTabletGateway_loadBalance_options_is_nil(t *testing.T) {
	tests := []struct {
		name       string
		candidates []*discovery.TabletHealth
		gw         *TabletGateway
		wantUid    uint32 // nolint:revive
	}{
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-500"},
				{uid: 4, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"},
				{uid: 3, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-300"},
				{uid: 2, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell"},
			wantUid: 5,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-500"},
				{uid: 4, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"},
				{uid: 3, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-300"},
				{uid: 2, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell"},
			wantUid: 3,
		},
		{
			name: "500 400 100 300 200",
			candidates: genTablets([]tabletInfo{
				{uid: 5, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:500"},
				{uid: 4, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:400"},
				{uid: 1, cell: "test_cell", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:100"},
				{uid: 3, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:300"},
				{uid: 2, cell: "test_cell2", position: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:200"},
			}),
			gw:      &TabletGateway{localCell: "test_cell2"},
			wantUid: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chosen := tt.gw.loadBalance(tt.candidates, nil)
			assert.NotNil(t, chosen)
			assert.NotZero(t, chosen.Tablet.Alias.Uid)
		})
	}
}
