package branch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wesql/wescale/endtoend/framework"
	"testing"
	"time"
)

func testSourceAndTargetClusterConnection(t *testing.T) {
	// Create context with 10-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Define retry interval
	retryInterval := 5 * time.Second

	// Channel for completion signal
	done := make(chan bool)

	// Run connection tests in goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				// Timeout or cancelled
				t.Error("Connection test timeout")
				done <- true
				return
			default:
				// Test source cluster connection
				err := sourceCluster.MysqlDb.Ping()
				if err != nil {
					t.Logf("Source cluster MySQL connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				err = sourceCluster.WescaleDb.Ping()
				if err != nil {
					t.Logf("Source cluster Wescale connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				// Test target cluster connection
				err = targetCluster.MysqlDb.Ping()
				if err != nil {
					t.Logf("Target cluster MySQL connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				err = targetCluster.WescaleDb.Ping()
				if err != nil {
					t.Logf("Target cluster Wescale connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				// All connections successful
				t.Log("All cluster connections test passed")
				done <- true
				return
			}
		}
	}()

	// Wait for test completion or timeout
	<-done
}

func sourcePrepare() {
	var sqlStatements = []string{
		"DROP DATABASE IF EXISTS test_db1;",
		"DROP DATABASE IF EXISTS test_db2;",
		"DROP DATABASE IF EXISTS test_db3;",

		"CREATE DATABASE test_db1;",
		"CREATE DATABASE test_db2;",
		"CREATE DATABASE test_db3;",

		`CREATE TABLE test_db1.users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`,

		`CREATE TABLE test_db2.orders (
        order_id INT PRIMARY KEY AUTO_INCREMENT,
        customer_name VARCHAR(100) NOT NULL,
        order_date DATE NOT NULL,
        total_amount DECIMAL(10,2),
        status VARCHAR(20)
    );`,

		`CREATE TABLE test_db3.source_products (
        product_id INT PRIMARY KEY AUTO_INCREMENT,
        product_name VARCHAR(200) NOT NULL,
        price DECIMAL(10,2),
        stock_quantity INT,
        category VARCHAR(50),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );`,
	}
	for _, statement := range sqlStatements {
		_, err := sourceCluster.WescaleDb.Exec(statement)
		if err != nil {
			panic(err)
		}
	}
}

func sourceClean() {
	var sqlStatements = []string{
		"DROP DATABASE IF EXISTS test_db1;",
		"DROP DATABASE IF EXISTS test_db2;",
		"DROP DATABASE IF EXISTS test_db3;",
	}
	for _, statement := range sqlStatements {
		_, err := sourceCluster.WescaleDb.Exec(statement)
		if err != nil {
			panic(err)
		}
	}
}

func targetPrepare() {
	var sqlStatements = []string{
		"DROP DATABASE IF EXISTS test_db1;",
		"DROP DATABASE IF EXISTS test_db2;",
		"DROP DATABASE IF EXISTS test_db3;",

		"CREATE DATABASE test_db3;",

		`CREATE TABLE test_db3.target_products (
        product_id INT PRIMARY KEY AUTO_INCREMENT,
        product_name VARCHAR(200) NOT NULL,
        price DECIMAL(10,2),
        stock_quantity INT,
        category VARCHAR(50),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );`,
	}
	for _, statement := range sqlStatements {
		_, err := targetCluster.WescaleDb.Exec(statement)
		if err != nil {
			panic(err)
		}
	}
}

func targetClean() {
	var sqlStatements = []string{
		"DROP DATABASE IF EXISTS test_db1;",
		"DROP DATABASE IF EXISTS test_db2;",
		"DROP DATABASE IF EXISTS test_db3;",
	}
	for _, statement := range sqlStatements {
		_, err := targetCluster.WescaleDb.Exec(statement)
		if err != nil {
			panic(err)
		}
	}
}

func getBranchCreateCMD(
	sourceHost string,
	sourcePort int,
	sourceUser string,
	sourcePassword string,
	includeDatabases string,
	excludeDatabases string,
) string {
	return fmt.Sprintf(`Branch create with (
    'source_host'='%s',
    'source_port'='%d',
    'source_user'='%s',
    'source_password'='%s',
    'include_databases'='%s',
    'exclude_databases'='%s'
);`,
		sourceHost,
		sourcePort,
		sourceUser,
		sourcePassword,
		includeDatabases,
		excludeDatabases,
	)
}

func getBranchCleanUpCMD() string {
	return fmt.Sprintf(`Branch clean_up;`)
}

// default override
func getBranchDiffCMD(compareObjects string) string {
	return fmt.Sprintf(`Branch diff with (
    'compare_objects'='%s'
);`, compareObjects)
}

// default override
func getBranchPrepareMergeBackCMD() string {
	return fmt.Sprintf(`Branch prepare_merge_back;`)
}

func getBranchMergeBackCMD() string {
	return fmt.Sprintf(`Branch merge_back;`)
}

func printBranchDiff(rows *sql.Rows) {
	fmt.Printf("---------------------- start printing branch diff ----------------------\n")
	for rows.Next() {
		var (
			database  string
			tableName string
			ddl       string
		)
		err := rows.Scan(&database, &tableName, &ddl)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Database: %s, Table: %s, DDL: %s\n", database, tableName, ddl)
	}
	fmt.Printf("---------------------- print branch diff end ----------------------\n")
}

func TestBranchBasic(t *testing.T) {
	testSourceAndTargetClusterConnection(t)
	sourcePrepare()
	targetPrepare()

	// defer cleanup
	defer framework.ExecNoError(t, targetCluster.WescaleDb, getBranchCleanUpCMD())
	defer sourceClean()
	defer targetClean()

	// create branch
	createCMD := getBranchCreateCMD(sourceHostToTarget, sourceCluster.MysqlPort, "root", "passwd", "*", "information_schema,mysql,performance_schema,sys")
	framework.ExecNoError(t, targetCluster.WescaleDb, createCMD)
	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db1", "users"))
	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db2", "orders"))
	// the test_db3 will be skipped when branch creating
	assert.Equal(t, false, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db3", "source_products"))
	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db3", "target_products"))

	// change schema
	framework.ExecNoError(t, sourceCluster.WescaleDb, "ALTER TABLE test_db3.source_products ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db3", "source_products", "description"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db3.target_products ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db3", "target_products", "description"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db1.users DROP COLUMN created_at;")
	assert.Equal(t, false, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db1", "users", "created_at"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db2.orders ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db2", "orders", "description"))

	// branch diff
	diffCMD := getBranchDiffCMD("source_target")
	rows := framework.QueryNoError(t, targetCluster.WescaleDb, diffCMD)
	defer rows.Close()
	printBranchDiff(rows)

	// branch prepare merge back
	rows2 := framework.QueryNoError(t, targetCluster.WescaleDb, getBranchPrepareMergeBackCMD())
	defer rows2.Close()
	printBranchDiff(rows2)

	// branch merge
	framework.ExecNoError(t, targetCluster.WescaleDb, getBranchMergeBackCMD())

	// no diff
	rows3 := framework.QueryNoError(t, targetCluster.WescaleDb, getBranchDiffCMD("source_target"))
	defer rows3.Close()
	assert.Equal(t, false, rows3.Next())

	// check schema
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db3", "target_products"))
	assert.Equal(t, false, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db3", "source_products"))
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db1", "users"))
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db2", "orders"))

	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db3", "target_products", "description"))
	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db2", "orders", "description"))

}

func TestBranchBasicWithFailPoint(t *testing.T) {
	testSourceAndTargetClusterConnection(t)
	sourcePrepare()
	targetPrepare()

	// defer cleanup
	defer framework.ExecNoError(t, targetCluster.WescaleDb, getBranchCleanUpCMD())
	defer sourceClean()
	defer targetClean()

	// create branch
	createCMD := getBranchCreateCMD(sourceCluster.MysqlHost, sourceCluster.MysqlPort, "root", "passwd", "*", "information_schema,mysql,performance_schema,sys")
	framework.EnableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchFetchSnapshotError", "return(true)")
	framework.ExecWithErrorContains(t, targetCluster.WescaleDb, "failpoint", createCMD)
	expectBranchStatus(t, "my_branch", "init")

	framework.DisableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchFetchSnapshotError")
	framework.EnableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchApplySnapshotError", "return(true)")
	framework.ExecWithErrorContains(t, targetCluster.WescaleDb, "failpoint", createCMD)
	expectBranchStatus(t, "my_branch", "fetched")
	framework.DisableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchApplySnapshotError")

	framework.ExecNoError(t, targetCluster.WescaleDb, createCMD)
	expectBranchStatus(t, "my_branch", "created")

	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db1", "users"))
	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db2", "orders"))
	// the test_db3 will be skipped when branch creating
	assert.Equal(t, false, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db3", "source_products"))
	assert.Equal(t, true, framework.CheckTableExists(t, targetCluster.WescaleDb, "test_db3", "target_products"))

	// change schema
	framework.ExecNoError(t, sourceCluster.WescaleDb, "ALTER TABLE test_db3.source_products ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db3", "source_products", "description"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db3.target_products ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db3", "target_products", "description"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db1.users DROP COLUMN created_at;")
	assert.Equal(t, false, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db1", "users", "created_at"))

	framework.ExecNoError(t, targetCluster.WescaleDb, "ALTER TABLE test_db2.orders ADD COLUMN description TEXT;")
	assert.Equal(t, true, framework.CheckColumnExists(t, targetCluster.WescaleDb, "test_db2", "orders", "description"))

	// branch diff
	diffCMD := getBranchDiffCMD("source_target")
	rows := framework.QueryNoError(t, targetCluster.WescaleDb, diffCMD)
	defer rows.Close()
	printBranchDiff(rows)

	// branch prepare merge back
	framework.EnableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchInsertMergeBackDDLError", "return(true)")
	framework.ExecWithErrorContains(t, targetCluster.WescaleDb, "failpoint", getBranchPrepareMergeBackCMD())
	expectBranchStatus(t, "my_branch", "preparing")
	framework.DisableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchInsertMergeBackDDLError")

	rows2 := framework.QueryNoError(t, targetCluster.WescaleDb, getBranchPrepareMergeBackCMD())
	defer rows2.Close()
	printBranchDiff(rows2)
	expectBranchStatus(t, "my_branch", "prepared")

	// branch merge
	framework.EnableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchExecuteMergeBackDDLError", "return(true)")
	framework.ExecWithErrorContains(t, targetCluster.WescaleDb, "failpoint", getBranchMergeBackCMD())
	expectBranchStatus(t, "my_branch", "merging")

	framework.DisableFailPoint(t, targetCluster.WescaleDb, "vitess.io/vitess/go/vt/vtgate/branch/BranchExecuteMergeBackDDLError")
	framework.ExecNoError(t, targetCluster.WescaleDb, getBranchMergeBackCMD())
	expectBranchStatus(t, "my_branch", "merged")

	// no diff
	rows3 := framework.QueryNoError(t, targetCluster.WescaleDb, getBranchDiffCMD("source_target"))
	defer rows3.Close()
	assert.Equal(t, false, rows3.Next())

	// check schema
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db3", "target_products"))
	assert.Equal(t, false, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db3", "source_products"))
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db1", "users"))
	assert.Equal(t, true, framework.CheckTableExists(t, sourceCluster.WescaleDb, "test_db2", "orders"))

	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db3", "target_products", "description"))
	assert.Equal(t, true, framework.CheckColumnExists(t, sourceCluster.WescaleDb, "test_db2", "orders", "description"))

}

func expectBranchStatus(t *testing.T, name, expectStatus string) {
	rows := framework.QueryNoError(t, targetCluster.WescaleDb, fmt.Sprintf("select status from mysql.branch where name = '%s'", name))
	defer rows.Close()
	assert.Equal(t, true, rows.Next())
	var actualStatus string
	err := rows.Scan(&actualStatus)
	assert.Nil(t, err)
	assert.Equal(t, expectStatus, actualStatus)
}
