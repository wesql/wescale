/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package viperutil

import (
	"strconv"

	"vitess.io/vitess/go/vt/vttablet/jobcontroller"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/role"
)

// todo: it seems registering the reload handlers is complicated, there are many duplicated code, need to refactor

// RegisterReloadHandlersForVtTablet
// viper_config will call these handlers when viper reloads a config file, even if the value remains the same
func RegisterReloadHandlersForVtTablet(v *ViperConfig, tsv *tabletserver.TabletServer) {
	v.ReloadHandler.AddReloadHandler("background_task_pool_size", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err == nil {
			tsv.SetTaskPoolSize(i)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_enable", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_dry_run", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_release_idle_connections_on_max_connection_error", DefaultFsReloadHandler)
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_percentage_of_max_connections", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err != nil {
			log.Errorf("cannot parse %s=%s as int", key, value)
			return
		}
		err = tabletserver.ValidatePercentageOfMaxConnections(i)
		if err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		DefaultFsReloadHandler(key, value, fs)
	})
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_safety_buffer", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err != nil {
			log.Errorf("cannot parse %s=%s as int", key, value)
			return
		}
		err = tabletserver.ValidateSafetyBuffer(i)
		if err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		DefaultFsReloadHandler(key, value, fs)
	})
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_tx_pool_percentage", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err != nil {
			log.Errorf("cannot parse %s=%s as int", key, value)
			return
		}
		err = tabletserver.ValidateTxPoolPercentage(i)
		if err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		DefaultFsReloadHandler(key, value, fs)
	})
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_min_tx_pool_size", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err != nil {
			log.Errorf("cannot parse %s=%s as int", key, value)
			return
		}
		err = tabletserver.ValidateMinTxPoolSize(i)
		if err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		DefaultFsReloadHandler(key, value, fs)
	})
	v.ReloadHandler.AddReloadHandler("queryserver_pool_autoscale_min_oltp_read_pool_size", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err != nil {
			log.Errorf("cannot parse %s=%s as int", key, value)
			return
		}
		err = tabletserver.ValidateMinOltpReadPoolSize(i)
		if err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
			return
		}
		DefaultFsReloadHandler(key, value, fs)
	})

	v.ReloadHandler.AddReloadHandler("mysql_role_probe_enable", DefaultFsReloadHandler)

	v.ReloadHandler.AddReloadHandler("mysql_role_probe_url_template", func(key string, value string, fs *pflag.FlagSet) {
		role.SetMysqlRoleProbeUrlTemplate(value)
		err := fs.Set("mysql_role_probe_url_template", value)
		if err != nil {
			log.Errorf("fail to set config mysql_role_probe_url_template=%s, err: %v", value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-pool-size", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err == nil {
			tsv.SetPoolSize(i)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-stream-pool-size", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err == nil {
			tsv.SetStreamPoolSize(i)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-transaction-cap", func(key string, value string, fs *pflag.FlagSet) {
		i, err := parseInt(key, value)
		if err == nil {
			tsv.SetTxPoolSize(i)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_default_batch_size", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetDefaultBatchSize(value); err == nil {
			_ = fs.Set("non_transactional_dml_default_batch_size", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_default_batch_interval", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetDefaultBatchInterval(value); err == nil {
			_ = fs.Set("non_transactional_dml_default_batch_interval", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_table_gc_interval", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetTableGCInterval(value); err == nil {
			_ = fs.Set("non_transactional_dml_table_gc_interval", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_job_manager_running_interval", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetJobManagerRunningInterval(value); err == nil {
			_ = fs.Set("non_transactional_dml_job_manager_running_interval", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_throttle_check_interval", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetThrottleCheckInterval(value); err == nil {
			_ = fs.Set("non_transactional_dml_throttle_check_interval", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_batch_size_threshold", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetBatchSizeThreshold(value); err == nil {
			_ = fs.Set("non_transactional_dml_batch_size_threshold", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("non_transactional_dml_batch_size_threshold_ratio", func(key string, value string, fs *pflag.FlagSet) {
		if err := jobcontroller.SetRatioOfBatchSizeThreshold(value); err == nil {
			_ = fs.Set("non_transactional_dml_batch_size_threshold_ratio", value)
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})
}

func parseInt(key, value string) (int, error) {
	i, err := strconv.Atoi(value)
	if err != nil {
		log.Errorf("cannot parse %s=%s as int", key, value)
		return 0, err
	}
	return i, nil
}
