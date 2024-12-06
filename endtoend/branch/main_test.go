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
var sourceHostToTarget string = "127.0.0.1"

func TestMain(m *testing.M) {
	sourceCluster = clusters.NewCustomSingleNodeCluster(
		// source cluster here is actually a mysql created by running ./init_mysql15307.sh, which it's convenient to debug the e2e test locally.
		// the source and target clusters info in github CI will be set based on config file in /vt/config/wescale/endtoend/branch
		"source",
		"127.0.0.1",
		15307,
		"root",
		"passwd",
		"127.0.0.1",
		15307,
		"root",
		"passwd",
	)
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

	flag.StringVar(&sourceHostToTarget, "source_host_to_target", "127.0.0.1", "the source host to target cluster, if target is in an separate env such as docker, it shouldn't be 127.0.0.1")
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
