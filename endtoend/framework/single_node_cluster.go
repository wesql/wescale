package framework

import (
	"database/sql"
	"flag"
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
	MysqlDb   *sql.DB
	WescaleDb *sql.DB
}

func SetUpSingleNodeCluster() (*SingleNodeCluster, error) {
	mysqlDb, err := newMysqlConnectionPool(mysqlHost, mysqlPort, mysqlUser, mysqlPasswd, "")
	if err != nil {
		return nil, err
	}

	wescaleDb, err := newMysqlConnectionPool(wescaleHost, wescalePort, wescaleUser, wescalePasswd, "")
	if err != nil {
		return nil, err
	}

	return &SingleNodeCluster{
		MysqlDb:   mysqlDb,
		WescaleDb: wescaleDb,
	}, nil
}

func (c *SingleNodeCluster) TearDownSingleNodeCluster() error {
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
