package viperutil

import (
	"strings"
)

func getRealKeyName(sectionAndKey string) string {
	alias := map[string]string{
		"table_acl_config_mode":               "table-acl-config-mode",
		"table_acl_config":                    "table-acl-config",
		"queryserver_config_strict_table_acl": "queryserver-config-strict-table-acl",
		"enforce_tableacl_config":             "enforce-tableacl-config",
		"table_acl_config_reload_interval":    "table-acl-config-reload-interval",
		"queryserver_config_pool_size":        "queryserver-config-pool-size",
		"queryserver_config_stream_pool_size": "queryserver-config-stream-pool-size",
		"queryserver_config_transaction_cap":  "queryserver-config-transaction-cap",
	}
	key := sectionAndKey
	if strings.Contains(sectionAndKey, ".") {
		// remove section from key
		key = strings.SplitN(sectionAndKey, ".", 2)[1]
	}

	if keyAlias, ok := alias[key]; ok {
		key = keyAlias
	}
	return key
}
