package wasm

import (
	_ "embed"
	"log"
	"os"
	"testing"
	"vitess.io/vitess/endtoend/framework"
)

//go:embed setup.sql
var setupSql string

//go:embed cleanup.sql
var cleanupSql string

var cluster *framework.SingleNodeCluster

func setup() error {
	var err error
	cluster, err = framework.SetUpSingleNodeCluster()
	if err != nil {
		return err
	}

	err = framework.ExecuteSqlScript(cluster.WescaleDb, setupSql)
	if err != nil {
		cluster.TearDownSingleNodeCluster()
		return err
	}

	return nil
}

func cleanup() error {
	if err := framework.ExecuteSqlScript(cluster.WescaleDb, cleanupSql); err != nil {
		return err
	}
	if err := cluster.TearDownSingleNodeCluster(); err != nil {
		return err
	}
	return nil
}

func TestMain(m *testing.M) {
	// Setup the test environment
	if err := setup(); err != nil {
		log.Printf("Setup failed: %v", err)
		os.Exit(1)
	}

	// Run the tests
	code := m.Run()

	// Cleanup the test environment
	if err := cleanup(); err != nil {
		log.Printf("Cleanup failed: %v", err)
		if code == 0 {
			os.Exit(1)
		}
	}

	os.Exit(code)
}
