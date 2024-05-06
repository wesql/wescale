package customrule

import (
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sidecardb"
)

var (
	DatabaseCustomRuleEnable            = true
	DatabaseCustomRuleDbName            = sidecardb.SidecarDBName
	DatabaseCustomRuleTableName         = "wescale_plugin"
	DatabaseCustomRuleReloadInterval    = 60 * time.Second
	DatabaseCustomRuleNotifierDelayTime = 100 * time.Millisecond
)

func registerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&DatabaseCustomRuleEnable, "database_custom_rule_enable", DatabaseCustomRuleEnable, "enable database custom rule")
	fs.StringVar(&DatabaseCustomRuleDbName, "database_custom_rule_db_name", DatabaseCustomRuleDbName, "sidecar db name for customrules file. default is mysql")
	fs.StringVar(&DatabaseCustomRuleTableName, "database_custom_rule_table_name", DatabaseCustomRuleTableName, "table name for customrules file. default is wescale_plugin")
	fs.DurationVar(&DatabaseCustomRuleReloadInterval, "database_custom_rule_reload_interval", DatabaseCustomRuleReloadInterval, "reload interval for customrules file. default is 60s")
}

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}
