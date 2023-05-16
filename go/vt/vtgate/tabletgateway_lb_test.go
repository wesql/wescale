/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"testing"

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
			chosen := gw.loadBalance(LEAST_GLOBAL_QPS, tt.candidates)
			if chosen == nil {
				assert.Equal(t, tt.wantQPS, -1.0)
				return
			}
			assert.Equalf(t, tt.wantQPS, chosen.Stats.Qps, "leastQpsLoadBalancer(%v, %v)", tt.candidates, tt.wantQPS)
		})
	}
}
