package framework

import (
	"database/sql"
	"flag"
	"fmt"
)

var (
	mysqlHost   = "127.0.0.1"
	mysqlPort   = 3306
	mysqlUser   = "root"
	mysqlPasswd = "passwd"

	wescaleHost   = "127.0.0.1"
	wescalePort   = 15306
	wescaleUser   = "root"
	wescalePasswd = "passwd"
)

func init() {
	RegisterFlagsForSingleNodeCluster()
}

func RegisterFlagsForSingleNodeCluster() {
	flag.StringVar(&mysqlHost, "mysqlHost", mysqlHost, "Host of the MySQL server")
	flag.IntVar(&mysqlPort, "mysqlPort", mysqlPort, "Port of the MySQL server")
	flag.StringVar(&mysqlUser, "mysqlUser", mysqlUser, "User for the MySQL server")
	flag.StringVar(&mysqlPasswd, "mysqlPasswd", mysqlPasswd, "Password for the MySQL server")

	flag.StringVar(&wescaleHost, "wescaleHost", wescaleHost, "Host of the WeScale server")
	flag.IntVar(&wescalePort, "wescalePort", wescalePort, "Port of the WeScale server")
	flag.StringVar(&wescaleUser, "wescaleUser", wescaleUser, "User for the WeScale server")
	flag.StringVar(&wescalePasswd, "wescalePasswd", wescalePasswd, "Password for the WeScale server")
}

type SingleNodeCluster struct {
	DbName        string
	SetUpScript   string
	CleanupScript string
	MysqlDb       *sql.DB
	WescaleDb     *sql.DB
}

// SetUpSingleNodeCluster creates a single node cluster with a MySQL and WeScale database.
// dbName can be an empty string if no database is needed.
// setupScript can be an empty string if no setup script is needed.
// cleanupScript can be an empty string if no cleanup script is needed.
func SetUpSingleNodeCluster(dbName string, setupScript string, cleanupScript string) (*SingleNodeCluster, error) {
	// Create the database
	db, err := newMysqlConnectionPool(wescaleHost, wescalePort, wescaleUser, wescalePasswd, "")
	if err != nil {
		return nil, err
	}
	if dbName != "" {
		_, err = db.Exec(fmt.Sprintf("create database if not exists `%s`", dbName))
		if err != nil {
			return nil, err
		}
	}

	// Create the connection pools
	mysqlDb, err := newMysqlConnectionPool(mysqlHost, mysqlPort, mysqlUser, mysqlPasswd, dbName)
	if err != nil {
		return nil, err
	}
	wescaleDb, err := newMysqlConnectionPool(wescaleHost, wescalePort, wescaleUser, wescalePasswd, dbName)
	if err != nil {
		return nil, err
	}

	// Execute Set Up Script
	if setupScript != "" {
		err = ExecuteSqlScript(wescaleDb, setupScript)
		if err != nil {
			return nil, err
		}
	}

	return &SingleNodeCluster{
		DbName:        dbName,
		SetUpScript:   setupScript,
		CleanupScript: cleanupScript,
		MysqlDb:       mysqlDb,
		WescaleDb:     wescaleDb,
	}, nil
}

func (c *SingleNodeCluster) TearDownSingleNodeCluster() error {
	// Execute Clean Up Script
	if c.CleanupScript != "" {
		err := ExecuteSqlScript(c.WescaleDb, c.CleanupScript)
		if err != nil {
			return err
		}
	}

	if c.MysqlDb != nil {
		err := c.MysqlDb.Close()
		if err != nil {
			return err
		}
	}
	if c.WescaleDb != nil {
		err := c.WescaleDb.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
