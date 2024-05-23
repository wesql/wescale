/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	"fmt"
	"testing"

	"github.com/wesql/wescale/go/vt/proto/query"

	"github.com/stretchr/testify/assert"
)

func TestIsRandom(t *testing.T) {
	assert.True(t, ReadWriteSplittingPolicyRandom.IsRandom())
	assert.False(t, ReadWriteSplittingPolicyDisable.IsRandom())
	assert.False(t, ReadWriteSplittingPolicy("").IsRandom())
	assert.True(t, ReadWriteSplittingPolicy("random").IsRandom())
	assert.False(t, ReadWriteSplittingPolicy("disable").IsRandom())
}

func TestParseReadWriteSplittingPolicy(t *testing.T) {
	type args struct {
		strategyVariable string
	}
	tests := []struct {
		name    string
		args    args
		want    *ReadWriteSplittingPolicySetting
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "random",
			args: args{
				strategyVariable: "random",
			},
			want: &ReadWriteSplittingPolicySetting{
				Strategy: ReadWriteSplittingPolicyRandom,
			},
			wantErr: assert.NoError,
		},
		{
			name: "RANDOM",
			args: args{
				strategyVariable: "RANDOM",
			},
			want: &ReadWriteSplittingPolicySetting{
				Strategy: ReadWriteSplittingPolicyRandom,
			},
			wantErr: assert.NoError,
		},
		{
			name: "disable",
			args: args{
				strategyVariable: "disable",
			},
			want: &ReadWriteSplittingPolicySetting{
				Strategy: ReadWriteSplittingPolicyDisable,
			},
			wantErr: assert.NoError,
		},
		{
			name: "empty",
			args: args{
				strategyVariable: "",
			},
			want: &ReadWriteSplittingPolicySetting{
				Strategy: ReadWriteSplittingPolicyDisable,
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseReadWriteSplittingPolicySetting(tt.args.strategyVariable)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseReadWriteSplittingPolicySetting(%v)", tt.args.strategyVariable)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ParseReadWriteSplittingPolicySetting(%v)", tt.args.strategyVariable)
		})
	}
}

func TestToLoadBalancePolicy(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want query.ExecuteOptions_LoadBalancePolicy
	}{
		{
			name: "random",
			args: args{
				s: "random",
			},
			want: query.ExecuteOptions_RANDOM,
		},
		{
			name: "empty",
			args: args{
				s: "",
			},
			want: query.ExecuteOptions_RANDOM,
		},
		{
			name: "LEAST_GLOBAL_QPS",
			args: args{
				s: "LEAST_GLOBAL_QPS",
			},
			want: query.ExecuteOptions_LEAST_GLOBAL_QPS,
		},
		{
			name: "LEAST_QPS",
			args: args{
				s: "LEAST_QPS",
			},
			want: query.ExecuteOptions_LEAST_QPS,
		},
		{
			name: "LEAST_RT",
			args: args{
				s: "LEAST_RT",
			},
			want: query.ExecuteOptions_LEAST_RT,
		},
		{
			name: "LEAST_BEHIND_PRIMARY",
			args: args{
				s: "LEAST_BEHIND_PRIMARY",
			},
			want: query.ExecuteOptions_LEAST_BEHIND_PRIMARY,
		},
		{
			name: "LEAST_MYSQL_CONNECTED_CONNECTIONS",
			args: args{
				s: "LEAST_MYSQL_CONNECTED_CONNECTIONS",
			},
			want: query.ExecuteOptions_LEAST_MYSQL_CONNECTED_CONNECTIONS,
		},
		{
			name: "LEAST_MYSQL_RUNNING_CONNECTIONS",
			args: args{
				s: "LEAST_MYSQL_RUNNING_CONNECTIONS",
			},
			want: query.ExecuteOptions_LEAST_MYSQL_RUNNING_CONNECTIONS,
		},
		{
			name: "LEAST_TABLET_INUSE_CONNECTIONS",
			args: args{
				s: "LEAST_TABLET_INUSE_CONNECTIONS",
			},
			want: query.ExecuteOptions_LEAST_TABLET_INUSE_CONNECTIONS,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ToLoadBalancePolicy(tt.args.s), "ToLoadBalancePolicy(%v)", tt.args.s)
		})
	}
}
