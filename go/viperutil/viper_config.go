/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

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

	vp                         *viper.Viper
	ConfigPath                 []string
	ConfigType                 string
	ConfigName                 string
	ConfigFileNotFoundHandling string
	Fs                         *pflag.FlagSet
	ReloadHandler              *Reloader
}

func NewViperConfig() *ViperConfig {
	return &ViperConfig{
		vp:            viper.New(),
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

	v.vp.SetConfigName(v.ConfigName)
	v.vp.SetConfigType(v.ConfigType)
	for _, p := range v.ConfigPath {
		v.vp.AddConfigPath(p)
	}
	err := v.vp.ReadInConfig()
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
	v.loadConfigFileAtStartup()
	v.startWatch()
}

func (v *ViperConfig) loadConfigFileAtStartup() {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()
	log.Infof("start refresh config file")
	for _, sectionAndKey := range v.vp.AllKeys() {
		key := getRealKeyName(sectionAndKey)
		value := v.vp.GetString(sectionAndKey)

		log.Infof("%s=%s", key, value)
		if v.Fs == nil {
			log.Exit("v.Fs is nil")
		}
<<<<<<< HEAD
		if err := v.Fs.Set(key, value); err != nil {
			log.Errorf("fail to set config %s=%s, err: %v", key, value, err)
		}
=======
		_ = v.Fs.Set(key, value)
>>>>>>> 0f5daf4b17 (fix: add_viper_handlers_for_some_feature_param)
	}
	log.Infof("finish refresh config file")
}

func (v *ViperConfig) reloadConfigs() {
	v.reloadMu.Lock()
	defer v.reloadMu.Unlock()
	log.Infof("start reload config file")
	for _, sectionAndKey := range v.vp.AllKeys() {
		key := getRealKeyName(sectionAndKey)
		value := v.vp.GetString(sectionAndKey)

		v.ReloadHandler.handleConfigChange(key, value, v.Fs)
	}
	log.Infof("finish reload config file")
}

func (v *ViperConfig) startWatch() {
	v.vp.OnConfigChange(func(e fsnotify.Event) {
		v.reloadConfigs()
	})
	v.vp.WatchConfig()
}
