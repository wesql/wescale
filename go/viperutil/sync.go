package viperutil

import (
	"fmt"
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
	ReloadHandler              *Reloader
}

func NewViperConfig() *ViperConfig {
	return &ViperConfig{
		ReloadHandler: NewConfigReloader(),
	}
}

func (v *ViperConfig) String() string {
	return fmt.Sprintf("ConfigPath=%s, ConfigType=%s, ConfigName=%s, ConfigFileNotFoundHandling=%s",
		v.ConfigPath, v.ConfigType, v.ConfigName, v.ConfigFileNotFoundHandling)
}

func (v *ViperConfig) LoadAndWatchConfigFile() {
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
	v.refreshConfigs()
	v.startWatch()
}

func (v *ViperConfig) refreshConfigs() {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()
	log.Infof("start refresh config file")
	for _, sectionAndKey := range viper.AllKeys() {
		key := getRealKeyName(sectionAndKey)
		value := viper.GetString(sectionAndKey)

		log.Infof("%s=%s", key, value)
		v.Fs.Set(key, value)
	}
	log.Infof("finish refresh config file")
}

func (v *ViperConfig) reloadConfigs() {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()
	log.Infof("start reload config file")
	for _, sectionAndKey := range viper.AllKeys() {
		key := getRealKeyName(sectionAndKey)
		value := viper.GetString(sectionAndKey)

		v.ReloadHandler.HandleConfigChange(key, value, v.Fs)
	}
	log.Infof("finish reload config file")
}

func (v *ViperConfig) startWatch() {
	viper.OnConfigChange(func(e fsnotify.Event) {
		v.reloadConfigs()
	})
	viper.WatchConfig()
}
