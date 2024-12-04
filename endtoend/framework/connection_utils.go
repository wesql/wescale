package framework

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
)

func newMysqlConfig(host string, port int, user string, passwd string, dbName string) *mysql.Config {
	return &mysql.Config{
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", host, port),
		User:                 user,
		Passwd:               passwd,
		AllowNativePasswords: true,
		DBName:               dbName,
	}
}

func NewMysqlConnectionPool(host string, port int, user string, passwd string, dbName string) (*sql.DB, error) {
	c := newMysqlConfig(host, port, user, passwd, dbName)
	c.MultiStatements = true
	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("%v:%v user:%v passwd:%v err:%v", host, port, user, passwd, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("%v:%v user:%v passwd:%v err:%v", host, port, user, passwd, err)
	}

	return db, nil
}
