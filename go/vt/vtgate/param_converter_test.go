/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wesql/wescale/go/vt/proto/vtgate"
)

func TestConvertReadAfterWriteConsistency(t *testing.T) {
	type args struct {
		readAfterWriteConsistency string
	}
	tests := []struct {
		name string
		args args
		want vtgate.ReadAfterWriteConsistency
	}{
		{
			name: "eventual",
			args: args{
				readAfterWriteConsistency: "eventual",
			},
			want: vtgate.ReadAfterWriteConsistency_EVENTUAL,
		},
		{
			name: "session",
			args: args{
				readAfterWriteConsistency: "session",
			},
			want: vtgate.ReadAfterWriteConsistency_SESSION,
		},
		{
			name: "instance",
			args: args{
				readAfterWriteConsistency: "instance",
			},
			want: vtgate.ReadAfterWriteConsistency_INSTANCE,
		},
		{
			name: "global",
			args: args{
				readAfterWriteConsistency: "global",
			},
			want: vtgate.ReadAfterWriteConsistency_GLOBAL,
		},
		{
			name: "foobar",
			args: args{
				readAfterWriteConsistency: "foobar",
			},
			want: vtgate.ReadAfterWriteConsistency_EVENTUAL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ConvertReadAfterWriteConsistency(tt.args.readAfterWriteConsistency), "ConvertReadAfterWriteConsistency(%v)", tt.args.readAfterWriteConsistency)
		})
	}
}

func TestValidateReadAfterWriteConsistency(t *testing.T) {
	type args struct {
		readAfterWriteConsistency string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "eventual",
			args: args{
				readAfterWriteConsistency: "eventual",
			},
			wantErr: assert.NoError,
		},
		{
			name: "session",
			args: args{
				readAfterWriteConsistency: "session",
			},
			wantErr: assert.NoError,
		},
		{
			name: "instance",
			args: args{
				readAfterWriteConsistency: "instance",
			},
			wantErr: assert.NoError,
		},
		{
			name: "global",
			args: args{
				readAfterWriteConsistency: "global",
			},
			wantErr: assert.NoError,
		},
		{
			name: "foobar",
			args: args{
				readAfterWriteConsistency: "foobar",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, ValidateReadAfterWriteConsistency(tt.args.readAfterWriteConsistency), fmt.Sprintf("ValidateReadAfterWriteConsistency(%v)", tt.args.readAfterWriteConsistency))
		})
	}
}
