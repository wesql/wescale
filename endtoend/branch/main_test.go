package branch

import (
	_ "embed"
	"flag"
	"github.com/wesql/wescale/endtoend/framework/clusters"
	"log"
	"os"
	"testing"
)

var dbName = "branch_e2e_test"

//go:embed setup.sql
var sourceSetupSql string

//go:embed cleanup.sql
var sourceCleanupSql string

//go:embed setup.sql
var targetSetupSql string

//go:embed cleanup.sql
var targetCleanupSql string

var sourceCluster *clusters.SingleNodeCluster
var targetCluster *clusters.SingleNodeCluster

func TestMain(m *testing.M) {
	// todo fix me after debugging branch
	sourceCluster = clusters.NewDefaultSingleNodeCluster()
	sourceCluster.RegisterFlagsForSingleNodeCluster()
	targetCluster = clusters.NewCustomSingleNodeCluster(
		"target",
		"127.0.0.1",
		3306,
		"root",
		"passwd",
		"127.0.0.1",
		15306,
		"root",
		"passwd",
	)
	targetCluster.RegisterFlagsForSingleNodeCluster()
	flag.Parse()

	// Setup the test environment
	err := sourceCluster.SetUp(dbName, sourceSetupSql)
	if err != nil {
		log.Fatalf("Setup failed: %v", err)
	}
	err = targetCluster.SetUp(dbName, targetSetupSql)
	if err != nil {
		log.Fatalf("Setup failed: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup the test environment
	err = sourceCluster.CleanUp(sourceCleanupSql)
	if err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}
	err = targetCluster.CleanUp(targetCleanupSql)
	if err != nil {
		log.Fatalf("Cleanup failed: %v", err)
	}

	os.Exit(code)
}
