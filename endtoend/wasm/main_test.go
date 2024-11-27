package wasm

import (
	_ "embed"
	"github.com/wesql/wescale/endtoend/framework"
	"log"
	"os"
	"testing"
)

var dbName = "wasm_e2e_test"

//go:embed setup.sql
var setupSql string

//go:embed cleanup.sql
var cleanupSql string

var cluster *framework.SingleNodeCluster

func TestMain(m *testing.M) {
	// Setup the test environment
	var err error
	cluster, err = framework.SetUpSingleNodeCluster(dbName, setupSql, cleanupSql)
	if err != nil {
		log.Fatalf("Setup failed: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup the test environment
	err = cluster.TearDownSingleNodeCluster()
	if err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}

	os.Exit(code)
}
