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
	v.ReloadHandler.AddReloadHandler("read_write_splitting_policy", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingPolicy(value); err == nil {
			err = fs.Set("read_write_splitting_policy", value)
			if err != nil {
				log.Errorf("fail to set config read_write_splitting_policy=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_write_splitting_ratio", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadWriteSplittingRatio(value); err == nil {
			err = fs.Set("read_write_splitting_ratio", value)
			if err != nil {
				log.Errorf("fail to set config read_write_splitting_ratio=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_consistency", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteConsistency(value); err == nil {
			err = fs.Set("read_after_write_consistency", value)
			if err != nil {
				log.Errorf("fail to set config read_after_write_consistency=%s, err: %v", value, err)
			}
		} else {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("read_after_write_timeout", func(key string, value string, fs *pflag.FlagSet) {
		if err := vtgate.SetDefaultReadAfterWriteTimeout(value); err == nil {
			err = fs.Set("read_after_write_timeout", value)
			if err != nil {
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
		if err := fs.Set("ddl_strategy", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
	})

	resolveTLS := func(fs *pflag.FlagSet) {
		mysqlSslKey, err := fs.GetString("mysql_server_ssl_key")
		if err != nil {
			log.Errorf("fail to get mysql_server_ssl_key, err: %v", err)
		}
		mysqlSslCert, err := fs.GetString("mysql_server_ssl_cert")
		if err != nil {
			log.Errorf("fail to get mysql_server_ssl_cert, err: %v", err)
		}
		requireSecure, err := fs.GetBool("mysql_server_require_secure_transport")
		if err != nil {
			log.Errorf("fail to get requireSecure, err: %v", err)
		}
		if mysqlSslCert != "" && mysqlSslKey != "" {
			vtgate.ReloadTLSConfig(mysqlSslCert, mysqlSslKey, requireSecure)
		}
	}

	v.ReloadHandler.AddReloadHandler("mysql_server_ssl_key", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_ssl_key", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		resolveTLS(fs)
	})
	v.ReloadHandler.AddReloadHandler("mysql_server_ssl_cert", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_ssl_cert", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		resolveTLS(fs)
	})
	v.ReloadHandler.AddReloadHandler("mysql_server_require_secure_transport", func(key string, value string, fs *pflag.FlagSet) {
		if err := fs.Set("mysql_server_require_secure_transport", value); err != nil {
			log.Errorf("fail to reload config %s=%s, err: %v", key, value, err)
		}
		resolveTLS(fs)
	})
}
