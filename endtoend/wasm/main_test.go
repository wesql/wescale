package wasm

import (
	_ "embed"
	"flag"
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
	cluster = framework.NewDefaultSingleNodeCluster()
	// Register flags for the single node cluster, allowing the user to override the default values
	cluster.RegisterFlagsForSingleNodeCluster()
	flag.Parse()

	// Setup the test environment
	err := cluster.SetUp(dbName, setupSql, cleanupSql)
	if err != nil {
		log.Fatalf("Setup failed: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup the test environment
	err = cluster.CleanUp()
	if err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}

	os.Exit(code)
}
