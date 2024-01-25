/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package viperutil

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/ini.v1"

	"vitess.io/vitess/go/vt/servenv"
)

func WriteVtGateConfig(filename string) error {
	// Open the file for writing, create it if not existing and truncate it if it does
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer file.Close()

	// Define the configuration content
	configContent := `[vtgate]
gateway_initial_tablet_timeout        = 30s
healthcheck_timeout                   = 2s
srv_topo_timeout                      = 1s
grpc_keepalive_time                   = 10s
grpc_keepalive_timeout                = 10s
tablet_refresh_interval               = 1m
read_write_splitting_policy           = least_rt
read_write_splitting_ratio            = 54
read_after_write_consistency          = GLOBAL
read_after_write_timeout              = 30.1
enable_buffer                         = true
buffer_size                           = 10000
buffer_window                         = 30s
buffer_max_failover_duration          = 60s
buffer_min_time_between_failovers     = 60s
mysql_auth_server_impl                = none
mysql_server_require_secure_transport = false
mysql_auth_server_static_file         = 
mysql_server_ssl_key                  = 
mysql_server_ssl_cert                 = 
`

	// Write the content to the file
	_, err = fmt.Fprint(file, configContent)
	if err != nil {
		return fmt.Errorf("unable to write configuration to file: %w", err)
	}

	return nil
}

func TestRegisterReloadHandlersForVtGate(t *testing.T) {
	require.NoError(t, WriteVtGateConfig("./test/vtgate.cnf"))

	vtGateViperConfig := NewViperConfig()
	fs := servenv.GetFlagSetFor("vtgate")
	fs.StringSliceVar(&vtGateViperConfig.ConfigPath, "config_path", []string{"./test"}, "Paths to search for config files in.")
	fs.StringVar(&vtGateViperConfig.ConfigType, "config_type", "ini", "Config file type (omit to infer config type from file extension).")
	fs.StringVar(&vtGateViperConfig.ConfigName, "config_name", "vtgate.cnf", "Name of the config file (without extension) to search for.")
	fs.StringVar(&vtGateViperConfig.ConfigFileNotFoundHandling, "config_file_not_found_handling", IGNORE, "Behavior when a config file is not found. (Options: IGNORE, ERROR, EXIT)")
	vtGateViperConfig.Fs = fs
	RegisterReloadHandlersForVtGate(vtGateViperConfig)

	vtGateViperConfig.LoadAndWatchConfigFile()

	{
		val, err := fs.GetString("read_write_splitting_policy")
		assert.NoError(t, err)
		assert.Equal(t, "least_rt", val)
	}

	{
		val, err := fs.GetInt("read_write_splitting_ratio")
		assert.NoError(t, err)
		assert.Equal(t, 54, val)
	}

	{
		_ = fs.Set("read_write_splitting_ratio", "28")
		val, err := fs.GetInt("read_write_splitting_ratio")
		assert.NoError(t, err)
		assert.Equal(t, 28, val)
	}

	{
		val, err := fs.GetString("read_after_write_consistency")
		assert.NoError(t, err)
		assert.Equal(t, "GLOBAL", val)
	}

	{
		val, err := fs.GetFloat64("read_after_write_timeout")
		assert.NoError(t, err)
		assert.Equal(t, 30.1, val)
	}
}

func TestRegisterReloadHandlersForVtGateWithModify(t *testing.T) {
	require.NoError(t, WriteVtGateConfig("./test/vtgate_test_modify.cnf"))

	vtGateViperConfig := NewViperConfig()
	fs := servenv.GetFlagSetFor("vtgate")
	fs.StringSliceVar(&vtGateViperConfig.ConfigPath, "config_path", []string{"./test"}, "Paths to search for config files in.")
	fs.StringVar(&vtGateViperConfig.ConfigType, "config_type", "ini", "Config file type (omit to infer config type from file extension).")
	fs.StringVar(&vtGateViperConfig.ConfigName, "config_name", "vtgate_test_modify.cnf", "Name of the config file (without extension) to search for.")
	fs.StringVar(&vtGateViperConfig.ConfigFileNotFoundHandling, "config_file_not_found_handling", IGNORE, "Behavior when a config file is not found. (Options: IGNORE, ERROR, EXIT)")
	vtGateViperConfig.Fs = fs

	vtGateViperConfig.LoadAndWatchConfigFile()

	// expect: read_after_write_timeout=30.1
	{
		val, err := fs.GetFloat64("read_after_write_timeout")
		assert.NoError(t, err)
		assert.Equal(t, 30.1, val)
	}

	// expect: mysql_server_ssl_cert=
	{
		val, err := fs.GetString("mysql_server_ssl_cert")
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}

	// expect: read_after_write_timeout=30.1
	// viper will reload config.
	// but since no reload handler was registered for 'read_after_write_timeout'
	// value for 'read_after_write_timeout' in fs will not change
	{
		configFileName := "./test/vtgate_test_modify.cnf"
		section := "vtgate"
		key := "read_after_write_timeout"
		value := "50.2"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetFloat64("read_after_write_timeout")
		assert.NoError(t, err)
		assert.Equal(t, 30.1, val)
	}

	RegisterReloadHandlersForVtGate(vtGateViperConfig)

	// expect: read_after_write_timeout=66.66
	// viper will reload config, and call reload handler to set 'read_after_write_timeout'
	{
		configFileName := "./test/vtgate_test_modify.cnf"
		section := "vtgate"
		key := "read_after_write_timeout"
		value := "66.66"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetFloat64("read_after_write_timeout")
		assert.NoError(t, err)
		assert.Equal(t, 66.66, val)
	}

	// expect: mysql_server_ssl_cert=
	// viper will reload config.
	// but since no reload handler was registered for 'mysql_server_ssl_cert'
	// value for 'mysql_server_ssl_cert' in fs will not change
	{
		configFileName := "./test/vtgate_test_modify.cnf"
		section := "vtgate"
		key := "mysql_server_ssl_cert"
		value := "foobar"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetString("mysql_server_ssl_cert")
		assert.NoError(t, err)
		assert.Equal(t, "", val)
	}

}

func SaveConfigTo(t *testing.T, configFileName, section, key, value string) {
	cfg, err := ini.Load(configFileName)
	assert.NoError(t, err)
	cfg.Section(section).Key(key).SetValue(value)
	err = cfg.SaveTo(configFileName)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
}
