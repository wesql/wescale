package framework

import (
	"database/sql"
	"flag"
	"fmt"
)

func RegisterFlagsForSingleNodeCluster(clusterName string, s *SingleNodeCluster) {
	flag.StringVar(&s.mysqlHost, fmt.Sprintf("%s_mysqlHost", clusterName), s.mysqlHost, "Host of the MySQL server")
	flag.IntVar(&s.mysqlPort, fmt.Sprintf("%s_mysqlPort", clusterName), s.mysqlPort, "Port of the MySQL server")
	flag.StringVar(&s.mysqlUser, fmt.Sprintf("%s_mysqlUser", clusterName), s.mysqlUser, "User for the MySQL server")
	flag.StringVar(&s.mysqlPasswd, fmt.Sprintf("%s_mysqlPasswd", clusterName), s.mysqlPasswd, "Password for the MySQL server")

	flag.StringVar(&s.wescaleHost, fmt.Sprintf("%s_wescaleHost", clusterName), s.wescaleHost, "Host of the WeScale server")
	flag.IntVar(&s.wescalePort, fmt.Sprintf("%s_wescalePort", clusterName), s.wescalePort, "Port of the WeScale server")
	flag.StringVar(&s.wescaleUser, fmt.Sprintf("%s_wescaleUser", clusterName), s.wescaleUser, "User for the WeScale server")
	flag.StringVar(&s.wescalePasswd, fmt.Sprintf("%s_wescalePasswd", clusterName), s.wescalePasswd, "Password for the WeScale server")
}

type SingleNodeCluster struct {
	mysqlHost   string
	mysqlPort   int
	mysqlUser   string
	mysqlPasswd string

	wescaleHost   string
	wescalePort   int
	wescaleUser   string
	wescalePasswd string

	DbName        string
	SetUpScript   string
	CleanupScript string

	MysqlDb   *sql.DB
	WescaleDb *sql.DB
}

func NewDefaultSingleNodeCluster() *SingleNodeCluster {
	return newSingleNodeCluster("default")
}

func newSingleNodeCluster(clusterName string) *SingleNodeCluster {
	s := &SingleNodeCluster{
		mysqlHost:     "127.0.0.1",
		mysqlPort:     3306,
		mysqlUser:     "root",
		mysqlPasswd:   "passwd",
		wescaleHost:   "127.0.0.1",
		wescalePort:   15306,
		wescaleUser:   "root",
		wescalePasswd: "passwd",
	}
	RegisterFlagsForSingleNodeCluster(clusterName, s)
	return s
}

// SetUpSingleNodeCluster creates a single node cluster with a MySQL and WeScale database.
// dbName can be an empty string if no database is needed.
// setupScript can be an empty string if no setup script is needed.
// cleanupScript can be an empty string if no cleanup script is needed.
func (s *SingleNodeCluster) SetUp(dbName string, setupScript string, cleanupScript string) (*SingleNodeCluster, error) {
	// Create the database
	db, err := newMysqlConnectionPool(s.wescaleHost, s.wescalePort, s.wescaleUser, s.wescalePasswd, "")
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
	mysqlDb, err := newMysqlConnectionPool(s.mysqlHost, s.mysqlPort, s.mysqlUser, s.mysqlPasswd, dbName)
	if err != nil {
		return nil, err
	}
	wescaleDb, err := newMysqlConnectionPool(s.wescaleHost, s.wescalePort, s.wescaleUser, s.wescalePasswd, dbName)
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

func (c *SingleNodeCluster) CleanUp() error {
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
