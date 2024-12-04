package branch

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
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

func TestBranchBasic(t *testing.T) {
	testSourceAndTargetClusterConnection(t)
	sourcePrepare()
	targetPrepare()

	// create branch
	createCMD := getBranchCreateCMD(sourceCluster.MysqlHost, sourceCluster.MysqlPort, "root", "passwd", "*", "information_schema,mysql,performance_schema,sys")
	_, err := targetCluster.WescaleDb.Exec(createCMD)
	assert.NoError(t, err)

	// defer branch cleanup
	defer targetCluster.WescaleDb.Exec("branch clean_up;")

	// change schema

	// branch diff

	// branch prepare merge back

	// branch merge
}
