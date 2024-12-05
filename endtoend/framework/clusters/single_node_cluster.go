package clusters

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/wesql/wescale/endtoend/framework"
)

// RegisterFlagsForSingleNodeCluster : Register flags for the single node cluster, allowing the user to override the default values
func (s *SingleNodeCluster) RegisterFlagsForSingleNodeCluster() {
	flag.StringVar(&s.MysqlHost, fmt.Sprintf("%s_mysqlHost", s.ClusterName), s.MysqlHost, "Host of the MySQL server")
	flag.IntVar(&s.MysqlPort, fmt.Sprintf("%s_mysqlPort", s.ClusterName), s.MysqlPort, "Port of the MySQL server")
	flag.StringVar(&s.MysqlUser, fmt.Sprintf("%s_mysqlUser", s.ClusterName), s.MysqlUser, "User for the MySQL server")
	flag.StringVar(&s.MysqlPasswd, fmt.Sprintf("%s_mysqlPasswd", s.ClusterName), s.MysqlPasswd, "Password for the MySQL server")

	flag.StringVar(&s.WescaleHost, fmt.Sprintf("%s_wescaleHost", s.ClusterName), s.WescaleHost, "Host of the WeScale server")
	flag.IntVar(&s.wescalePort, fmt.Sprintf("%s_wescalePort", s.ClusterName), s.wescalePort, "Port of the WeScale server")
	flag.StringVar(&s.wescaleUser, fmt.Sprintf("%s_wescaleUser", s.ClusterName), s.wescaleUser, "User for the WeScale server")
	flag.StringVar(&s.wescalePasswd, fmt.Sprintf("%s_wescalePasswd", s.ClusterName), s.wescalePasswd, "Password for the WeScale server")
}

type SingleNodeCluster struct {
	ClusterName string

	MysqlHost   string
	MysqlPort   int
	MysqlUser   string
	MysqlPasswd string

	WescaleHost   string
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
	return NewCustomSingleNodeCluster(
		"default",
		"127.0.0.1",
		3306,
		"root",
		"passwd",
		"127.0.0.1",
		15306,
		"root",
		"passwd",
	)
}

func NewCustomSingleNodeCluster(clusterName string,
	mysqlHost string, mysqlPort int, mysqlUser string, mysqlPasswd string,
	wescaleHost string, wescalePort int, wescaleUser string, wescalePasswd string) *SingleNodeCluster {
	s := &SingleNodeCluster{
		ClusterName:   clusterName,
		MysqlHost:     mysqlHost,
		MysqlPort:     mysqlPort,
		MysqlUser:     mysqlUser,
		MysqlPasswd:   mysqlPasswd,
		WescaleHost:   wescaleHost,
		wescalePort:   wescalePort,
		wescaleUser:   wescaleUser,
		wescalePasswd: wescalePasswd,
	}
	return s
}

// SetUpSingleNodeCluster creates a single node cluster with a MySQL and WeScale database.
// dbName can be an empty string if no database is needed.
// setupScript can be an empty string if no setup script is needed.
// cleanupScript can be an empty string if no cleanup script is needed.
func (s *SingleNodeCluster) SetUp(dbName string, setupScript string) error {
	// Create the database
	db, err := framework.NewMysqlConnectionPool(s.WescaleHost, s.wescalePort, s.wescaleUser, s.wescalePasswd, "")
	if err != nil {
		return err
	}
	defer db.Close()
	if dbName != "" {
		_, err = db.Exec(fmt.Sprintf("create database if not exists `%s`", dbName))
		if err != nil {
			return err
		}
	}

	// Create the connection pools
	mysqlDb, err := framework.NewMysqlConnectionPool(s.MysqlHost, s.MysqlPort, s.MysqlUser, s.MysqlPasswd, dbName)
	if err != nil {
		return err
	}
	wescaleDb, err := framework.NewMysqlConnectionPool(s.WescaleHost, s.wescalePort, s.wescaleUser, s.wescalePasswd, dbName)
	if err != nil {
		return err
	}

	// Execute Set Up Script
	if setupScript != "" {
		err = framework.ExecuteSqlScript(wescaleDb, setupScript)
		if err != nil {
			return err
		}
	}

	s.DbName = dbName
	s.SetUpScript = setupScript
	s.MysqlDb = mysqlDb
	s.WescaleDb = wescaleDb
	return nil
}

func (s *SingleNodeCluster) CleanUp(cleanupScript string) error {
	s.CleanupScript = cleanupScript
	// Execute Clean Up Script
	if s.CleanupScript != "" {
		err := framework.ExecuteSqlScript(s.WescaleDb, s.CleanupScript)
		if err != nil {
			return err
		}
	}

	if s.MysqlDb != nil {
		err := s.MysqlDb.Close()
		if err != nil {
			return err
		}
	}
	if s.WescaleDb != nil {
		err := s.WescaleDb.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
