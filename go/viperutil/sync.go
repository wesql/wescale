package viperutil

import (
	"fmt"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/vt/log"
)

const (
	IGNORE = "IGNORE"
	ERROR  = "ERROR"
	EXIT   = "EXIT"
)

type ViperConfig struct {
	mu       sync.Mutex
	reloadMu sync.Mutex

	ConfigPath                 []string
	ConfigType                 string
	ConfigName                 string
	ConfigFileNotFoundHandling string
	Fs                         *pflag.FlagSet
}

func NewViperConfig() *ViperConfig {
	return &ViperConfig{}
}

func (v *ViperConfig) String() string {
	return fmt.Sprintf("ConfigPath=%s, ConfigType=%s, ConfigName=%s, ConfigFileNotFoundHandling=%s",
		v.ConfigPath, v.ConfigType, v.ConfigName, v.ConfigFileNotFoundHandling)
}

func (v *ViperConfig) WatchConfigFile() {
	v.mu.Lock()
	defer v.mu.Unlock()

	viper.SetConfigName(v.ConfigName)
	viper.SetConfigType(v.ConfigType)
	for _, p := range v.ConfigPath {
		viper.AddConfigPath(p)
	}
	err := viper.ReadInConfig()
	if err != nil {
		switch v.ConfigFileNotFoundHandling {
		case IGNORE:
			log.Infof("ViperConfig: %v", v)
			log.Infof("read config file error, err: %v", err)
		case ERROR:
			log.Errorf("ViperConfig: %v", v)
			log.Errorf("read config file error, err: %v", err)
		case EXIT:
			log.Errorf("ViperConfig: %v", v)
			log.Exitf("read config file error, err: %v", err)
		}
	}
	v.reloadConfigs()
	viper.OnConfigChange(func(e fsnotify.Event) {
		v.reloadConfigs()
	})
	viper.WatchConfig()
}

func (v *ViperConfig) reloadConfigs() {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()
	log.Infof("start reload config file")
	for _, sectionAndKey := range viper.AllKeys() {
		key := sectionAndKey
		if strings.Contains(sectionAndKey, ".") {
			// remove section from key
			key = strings.SplitN(sectionAndKey, ".", 2)[1]
		}
		value := viper.GetString(sectionAndKey)
		log.Infof("%s=%s", key, value)

		if keyAlias, ok := alias[key]; ok {
			key = keyAlias
		}

		v.Fs.Set(key, value)
	}
	log.Infof("finish reload config file")
}

var alias = map[string]string{
	"table_acl_config_mode":               "table-acl-config-mode",
	"table_acl_config":                    "table-acl-config",
	"queryserver_config_strict_table_acl": "queryserver-config-strict-table-acl",
	"enforce_tableacl_config":             "enforce-tableacl-config",
	"table_acl_config_reload_interval":    "table-acl-config-reload-interval",
	"queryserver_config_pool_size":        "queryserver-config-pool-size",
	"queryserver_config_stream_pool_size": "queryserver-config-stream-pool-size",
	"queryserver_config_transaction_cap":  "queryserver-config-transaction-cap",
}
