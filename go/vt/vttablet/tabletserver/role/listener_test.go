package role

import (
	"os"
	"testing"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func Test_setUpMysqlProbeServicePort(t *testing.T) {
	tests := []struct {
		name        string
		expectPort  int64
		prepareFunc func()
	}{
		{
			"test empty LORRY_HTTP_PORT_ENV_NAME",
			3501,
			func() {
				//do nothing
			},
		},
		{
			"test LORRY_HTTP_PORT_ENV_NAME injection",
			9999,
			func() {
				os.Setenv(LORRY_HTTP_PORT_ENV_NAME, "9999")
			},
		},
		{
			"test LORRY_HTTP_PORT_ENV_NAME injection incorrect value",
			3501,
			func() {
				os.Setenv(LORRY_HTTP_PORT_ENV_NAME, "foobar")
			},
		},
		{
			"test LORRY_HTTP_PORT_ENV_NAME injection empty value",
			3501,
			func() {
				os.Setenv(LORRY_HTTP_PORT_ENV_NAME, "")
			},
		},
		{
			"test LORRY_HTTP_PORT_ENV_NAME injection blank value",
			3501,
			func() {
				os.Setenv(LORRY_HTTP_PORT_ENV_NAME, "   ")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareFunc()
			setUpMysqlProbeServicePort()
		})
	}
}

func Test_transitionRoleType(t *testing.T) {
	tests := []struct {
		role string
		want topodata.TabletType
	}{
		{
			role: "primary",
			want: topodata.TabletType_PRIMARY,
		},
		{
			role: "PriMARY",
			want: topodata.TabletType_PRIMARY,
		},
		{
			role: "secondary",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "Secondary",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "SecondarY",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "follower",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "slave",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "Slave",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "SLAVE",
			want: topodata.TabletType_REPLICA,
		},
		{
			role: "SLAVE2",
			want: topodata.TabletType_UNKNOWN,
		},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			if got := transitionRoleType(tt.role); got != tt.want {
				t.Errorf("transitionRoleType() = %v, want %v", got, tt.want)
			}
		})
	}
}
