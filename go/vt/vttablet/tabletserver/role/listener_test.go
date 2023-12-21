package role

import (
	"os"
	"testing"
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
