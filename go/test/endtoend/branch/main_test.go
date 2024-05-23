/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package branch

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/wesql/wescale/go/mysql"
	"github.com/wesql/wescale/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	opOrder               int64
	opOrderMutex          sync.Mutex
	onlineDDLStrategy     = "vitess"
	hostname              = "localhost"
	keyspaceName          = "mysql"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	cleanupStatements     = []string{
		`DROP TABLE IF EXISTS stress_test`,
	}
	createStatement = `
		CREATE TABLE stress_test (
			id bigint(20) not null,
			rand_val varchar(32) null default '',
			op_order bigint unsigned not null default 0,
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
	`
	alterHintStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default '%s'
	`
	insertRowStatement = `
		INSERT IGNORE INTO stress_test (id, rand_val, op_order) VALUES (%d, left(md5(rand()), 8), %d)
	`
	updateRowStatement = `
		UPDATE stress_test SET op_order=%d, updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d AND updates=1
	`
	selectMaxOpOrder = `
		SELECT MAX(op_order) as m FROM stress_test
	`
	// We use CAST(SUM(updates) AS SIGNED) because SUM() returns a DECIMAL datatype, and we want to read a SIGNED INTEGER type
	selectCountRowsStatement = `
		SELECT COUNT(*) AS num_rows, CAST(SUM(updates) AS SIGNED) AS sum_updates FROM stress_test
	`
	truncateStatement = `
		TRUNCATE TABLE stress_test
	`
)

const (
	maxTableRows                  = 4096
	maxConcurrency                = 5
	singleConnectionSleepInterval = 2 * time.Millisecond
	countIterations               = 5
	migrationWaitTimeout          = 120 * time.Second
	userCount                     = 5000
	productCount                  = 5000
	customerCount                 = 10000
	corderCount                   = 5000
)

var mysqlConn *mysql.Conn

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	code := runAllTests(m)
	os.Exit(code)
}
func runAllTests(m *testing.M) int {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema_change_dir", schemaChangeDirectory,
			"--schema_change_controller", "local",
			"--schema_change_check_interval", "1",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "5s",
			"--watch_replication_stream",
		}
		clusterInstance.VtGateExtraArgs = []string{}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		// No need for replicas in this stress test
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"0"}, 0, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: keyspaceName,
		}

		return m.Run(), nil
	}()
	if err != nil {
		log.Fatalf("%v", err)
	}
	return exitcode
}
