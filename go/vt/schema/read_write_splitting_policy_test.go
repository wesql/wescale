/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	"fmt"
	"testing"

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
