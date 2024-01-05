package viperutil

import (
	"github.com/spf13/pflag"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate"
)

// RegisterReloadHandlersForVtGate
// viper_config will call these handlers when viper reloads a config file, even if the value remains the same
func RegisterReloadHandlersForVtGate(v *ViperConfig) {
	v.ReloadHandler.AddReloadHandler("read_write_splitting_policy", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingPolicy(value); err == nil {
			fs.Set("read_write_splitting_policy", value)
		} else {
			log.Errorf("fail to reload config %s=%s", key, value)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_write_splitting_ratio", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingRatio(value); err == nil {
			fs.Set("read_write_splitting_ratio", value)
		} else {
			log.Errorf("fail to reload config %s=%s", key, value)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_consistency", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteConsistency(value); err == nil {
			fs.Set("read_after_write_consistency", value)
		} else {
			log.Errorf("fail to reload config %s=%s", key, value)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_timeout", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteTimeout(value); err == nil {
			fs.Set("read_after_write_timeout", value)
		} else {
			log.Errorf("fail to reload config %s=%s", key, value)
		}
	})
}
