package viperutil

import "testing"

func Test_getRealKeyName(t *testing.T) {
	tests := []struct {
		sectionAndKey string
		want          string
	}{
		{
			"vtgate.enable_buffer",
			"enable_buffer",
		},
		{
			"vtgate.read_write_splitting_policy",
			"read_write_splitting_policy",
		},
		{
			"vttablet.queryserver_config_pool_size",
			"queryserver-config-pool-size",
		},
	}
	for _, tt := range tests {
		t.Run(tt.sectionAndKey, func(t *testing.T) {
			if got := getRealKeyName(tt.sectionAndKey); got != tt.want {
				t.Errorf("getRealKeyName() = %v, want %v", got, tt.want)
			}
		})
	}
}
