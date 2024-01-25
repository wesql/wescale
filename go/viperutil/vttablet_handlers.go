/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package viperutil

import (
	"strconv"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/role"
)

// RegisterReloadHandlersForVtTablet
// viper_config will call these handlers when viper reloads a config file, even if the value remains the same
func RegisterReloadHandlersForVtTablet(v *ViperConfig, tsv *tabletserver.TabletServer) {
	v.ReloadHandler.AddReloadHandler("mysql_role_probe_url_template", func(key string, value string, fs *pflag.FlagSet) {
		role.SetMysqlRoleProbeUrlTemplate(value)
		err := fs.Set("mysql_role_probe_url_template", value)
		if err != nil {
			log.Errorf("fail to set config mysql_role_probe_url_template=%s, err: %v", value, err)
		}
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-pool-size", func(key string, value string, fs *pflag.FlagSet) {
		tsv.SetPoolSize(parseInt(key, value))
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-stream-pool-size", func(key string, value string, fs *pflag.FlagSet) {
		tsv.SetStreamPoolSize(parseInt(key, value))
	})

	v.ReloadHandler.AddReloadHandler("queryserver-config-transaction-cap", func(key string, value string, fs *pflag.FlagSet) {
		tsv.SetTxPoolSize(parseInt(key, value))
	})
}

func parseInt(key, value string) int {
	i, err := strconv.Atoi(value)
	if err != nil {
		log.Errorf("cannot parse %s=%s as int", key, value)
	}
	return i
}
