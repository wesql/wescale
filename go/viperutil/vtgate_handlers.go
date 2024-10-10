/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package viperutil

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vtgate"
)

// RegisterReloadHandlersForVtGate
// viper_config will call these handlers when viper reloads a config file, even if the value remains the same
func RegisterReloadHandlersForVtGate(v *ViperConfig) {
	v.ReloadHandler.AddReloadHandler("enable_auto_suspend", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_suspend_timeout", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("enable_auto_scale", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_decision_making_interval", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_compute_unit_lower_bound", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_compute_unit_upper_bound", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_cpu_ratio", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_memory_ratio", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_use_relaxed_cpu_memory_ratio", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_cluster_namespace", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_data_node_pod_name", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_data_node_stateful_set_name", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_logger_node_pod_name", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_logger_node_stateful_set_name", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("auto_scale_vtgate_headless_service_name", DefaultFsReloadHandler)

	v.ReloadHandler.AddReloadHandler("read_write_splitting_policy", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingPolicy(value); err == nil {
			if err = fs.Set("read_write_splitting_policy", value); err != nil {
				log.Errorf("fail to set config read_write_splitting_policy=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_write_splitting_ratio", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingRatio(value); err == nil {
			if err = fs.Set("read_write_splitting_ratio", value); err != nil {
				log.Errorf("fail to set config read_write_splitting_ratio=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_consistency", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteConsistency(value); err == nil {
			if err = fs.Set("read_after_write_consistency", value); err != nil {
				log.Errorf("fail to set config read_after_write_consistency=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_timeout", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteTimeout(value); err == nil {
			if err = fs.Set("read_after_write_timeout", value); err != nil {
				log.Errorf("fail to set config read_after_write_timeout=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("ddl_strategy", func(key string, value string, fs *pflag.FlagSet) {
		if _, err := schema.ParseDDLStrategy(value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		if err := fs.Set("ddl_strategy", value); err == nil {
			_ = fs.Set("ddl_strategy", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("enable_display_sql_execution_vttablets", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultEnableDisplaySQLExecutionVTTablet(value); err == nil {
			_ = fs.Set("enable_display_sql_execution_vttablets", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("enable_read_write_split_for_read_only_txn", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplitForReadOnlyTxnUserInput(value); err == nil {
			_ = fs.Set("enable_read_write_split_for_read_only_txn", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("mysql_server_ssl_key", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_ssl_key", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		vtgate.ReloadTLSConfig()
	})
	v.ReloadHandler.AddReloadHandler("mysql_server_ssl_cert", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_ssl_cert", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		vtgate.ReloadTLSConfig()
	})
	v.ReloadHandler.AddReloadHandler("mysql_server_require_secure_transport", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_require_secure_transport", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		vtgate.ReloadTLSConfig()
	})
	v.ReloadHandler.AddReloadHandler("mysql_server_ssl_ca", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_ssl_ca", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		vtgate.ReloadTLSConfig()
	})
}
