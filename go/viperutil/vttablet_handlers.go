package viperutil

import (
	"github.com/spf13/pflag"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/role"
)

// RegisterReloadHandlersForVtTablet
// viper_config will call these handlers when viper reloads a config file, even if the value remains the same
func RegisterReloadHandlersForVtTablet(v *ViperConfig) {
	v.ReloadHandler.AddReloadHandler("mysql_role_probe_url_template", func(key string, value string, fs *pflag.FlagSet) {
		role.SetMysqlRoleProbeUrlTemplate(value)
		fs.Set("mysql_role_probe_url_template", value)
	})
	v.ReloadHandler.AddReloadHandler("queryserver-config-pool-size", func(key string, value string, fs *pflag.FlagSet) {
		log.Info("reload but ignore: %s=%s", key, value)
	})
}
