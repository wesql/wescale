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

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
)

func WriteVttabletConfig(filename string) error {
	// Open the file for writing, create it if not existing and truncate it if it does
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	defer file.Close()

	// Define the configuration content
	configContent := `[vttablet]
health_check_interval               = 1s
shard_sync_retry_delay              = 1s
remote_operation_timeout            = 1s
db_connect_timeout_ms               = 500
table_acl_config_mode               = simple
enable_logs                         = true
enable_query_log                    = true
table_acl_config                    = 
queryserver_config_strict_table_acl = false
table_acl_config_reload_interval    = 30s
enforce_tableacl_config             = false

# the size of database connection pool in non transaction dml
non_transactional_dml_database_pool_size=3
# the number of rows to be processed in one batch by default
non_transactional_dml_default_batch_size=2000
# the interval of batch processing in milliseconds by default
non_transactional_dml_default_batch_interval=1
# the interval of table GC in hours
non_transactional_dml_table_gc_interval=24
# the interval of job scheduler running in seconds
non_transactional_dml_job_manager_running_interval=24
# the interval of throttle check in milliseconds
non_transactional_dml_throttle_check_interval=250
# the threshold of batch size
non_transactional_dml_batch_size_threshold=10000
# final threshold = ratio * non_transactional_dml_batch_size_threshold / table index numbers
non_transactional_dml_batch_size_threshold_ratio=0.5
`

	// Write the content to the file
	_, err = fmt.Fprint(file, configContent)
	if err != nil {
		return fmt.Errorf("unable to write configuration to file: %w", err)
	}

	return nil
}

func TestRegisterReloadHandlersForVtTablet(t *testing.T) {
	require.NoError(t, WriteVttabletConfig("./test/vttablet.cnf"))

	if false {
		log.Info(tabletserver.DTStateCommit)
	}

	vtTabletViperConfig := NewViperConfig()
	fs := servenv.GetFlagSetFor("vttablet")
	fs.StringSliceVar(&vtTabletViperConfig.ConfigPath, "config_path", []string{"./test"}, "Paths to search for config files in.")
	fs.StringVar(&vtTabletViperConfig.ConfigType, "config_type", "ini", "Config file type (omit to infer config type from file extension).")
	fs.StringVar(&vtTabletViperConfig.ConfigName, "config_name", "vttablet.cnf", "Name of the config file (without extension) to search for.")
	fs.StringVar(&vtTabletViperConfig.ConfigFileNotFoundHandling, "config_file_not_found_handling", IGNORE, "Behavior when a config file is not found. (Options: IGNORE, ERROR, EXIT)")
	vtTabletViperConfig.Fs = fs
	RegisterReloadHandlersForVtTablet(vtTabletViperConfig, nil)

	vtTabletViperConfig.LoadAndWatchConfigFile()

	{
		val, err := fs.GetDuration("health_check_interval")
		assert.NoError(t, err)
		assert.Equal(t, time.Second, val)
	}

	{
		val, err := fs.GetInt("non_transactional_dml_job_manager_running_interval")
		assert.NoError(t, err)
		assert.Equal(t, 24, val)
	}

	{
		val, err := fs.GetFloat64("non_transactional_dml_batch_size_threshold_ratio")
		assert.NoError(t, err)
		assert.Equal(t, 0.5, val)
	}
}

func TestRegisterReloadHandlersForVtTabletWithModify(t *testing.T) {
	require.NoError(t, WriteVttabletConfig("./test/vttablet_test_modify.cnf"))

	if false {
		log.Info(tabletserver.DTStateCommit)
	}

	vtTabletViperConfig := NewViperConfig()
	fs := servenv.GetFlagSetFor("vttablet")
	fs.StringSliceVar(&vtTabletViperConfig.ConfigPath, "config_path", []string{"./test"}, "Paths to search for config files in.")
	fs.StringVar(&vtTabletViperConfig.ConfigType, "config_type", "ini", "Config file type (omit to infer config type from file extension).")
	fs.StringVar(&vtTabletViperConfig.ConfigName, "config_name", "vttablet_test_modify.cnf", "Name of the config file (without extension) to search for.")
	fs.StringVar(&vtTabletViperConfig.ConfigFileNotFoundHandling, "config_file_not_found_handling", IGNORE, "Behavior when a config file is not found. (Options: IGNORE, ERROR, EXIT)")
	vtTabletViperConfig.Fs = fs

	//RegisterReloadHandlersForVtTablet(vtTabletViperConfig, nil)
	vtTabletViperConfig.LoadAndWatchConfigFile()

	//expect: mysql_role_probe_url_template=http://%s:%d/v1.0/getrole
	{
		val, err := fs.GetString("mysql_role_probe_url_template")
		assert.NoError(t, err)
		assert.Equal(t, "http://%s:%d/v1.0/getrole", val)
	}

	// expect: mysql_role_probe_url_template=http://%s:%d/v1.0/getrole
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "mysql_role_probe_url_template"
		value := "http://%s:%d/v2.0/getrole"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetString("mysql_role_probe_url_template")
		assert.NoError(t, err)
		assert.Equal(t, "http://%s:%d/v1.0/getrole", val)
	}

	RegisterReloadHandlersForVtTablet(vtTabletViperConfig, nil)

	// expect: mysql_role_probe_url_template=http://%s:%d/v2.0/getrole
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "mysql_role_probe_url_template"
		value := "http://%s:%d/v3.0/getrole"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetString("mysql_role_probe_url_template")
		assert.NoError(t, err)
		assert.Equal(t, "http://%s:%d/v3.0/getrole", val)
	}

	// expect: non_transactional_dml_database_pool_size = 3(default value), because non_transactional_dml_database_pool_size should >= 1
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "non_transactional_dml_database_pool_size"
		value := "0"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetInt("non_transactional_dml_database_pool_size")
		assert.NoError(t, err)
		assert.Equal(t, 3, val)
	}

	// expect: non_transactional_dml_database_pool_size = 3(default value), because non_transactional_dml_database_pool_size should >= 1
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "non_transactional_dml_database_pool_size"
		value := "1.2345"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetInt("non_transactional_dml_database_pool_size")
		assert.NoError(t, err)
		assert.Equal(t, 3, val)
	}

	// expect: non_transactional_dml_database_pool_size = 1
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "non_transactional_dml_database_pool_size"
		value := "1"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetInt("non_transactional_dml_database_pool_size")
		assert.NoError(t, err)
		assert.Equal(t, 1, val)
	}

	// expect: non_transactional_dml_batch_size_threshold_ratio = 0.85
	{
		configFileName := "./test/vttablet_test_modify.cnf"
		section := "vttablet"
		key := "non_transactional_dml_batch_size_threshold_ratio"
		value := "0.85"
		SaveConfigTo(t, configFileName, section, key, value)

		val, err := fs.GetFloat64("non_transactional_dml_batch_size_threshold_ratio")
		assert.NoError(t, err)
		assert.Equal(t, 0.85, val)
	}

}
