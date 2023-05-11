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
				availableTablets: generateTabletHealth(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "",
				},
			},
			want: generateTabletHealth(
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
				availableTablets: generateTabletHealth(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealth(
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
				availableTablets: generateTabletHealth(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want:    generateTabletHealth("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
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
				availableTablets: generateTabletHealth(
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
				availableTablets: generateTabletHealth(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealth(
				"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				"ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
		},
		{
			name: "get partial availableTablets",
			lastSeenGtid: &LastSeenGtid{
				flavor: mysql.Mysql56FlavorID,
			},
			args: args{
				availableTablets: generateTabletHealth(
					"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
					"ddfabe04-d9b4-11ed-8345-d22027637c46:1"),
				options: &querypb.ExecuteOptions{
					ReadAfterWriteGtid: "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100",
				},
			},
			want: generateTabletHealth("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"),
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

func generateTabletHealth(positions ...string) []*discovery.TabletHealth {
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
