/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func InitTable(conn *mysql.Conn) {
	conn.ExecuteFetch("CREATE DATABASE test", 1000, false)
	conn.ExecuteFetch("use test", 1000, false)
	conn.ExecuteFetch("CREATE TABLE t1 (v1 INT AUTO_INCREMENT PRIMARY KEY,  v2 INT);", 1000, false)
	conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
}

func CreateVtParam(username, host, password string) mysql.ConnParams {
	return mysql.ConnParams{
		Host:  host,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: username,
		Pass:  password,
	}
}
func CreateUser(conn *mysql.Conn, username, host, password string) error {
	sql := fmt.Sprintf("CREATE USER '%s'@'%s' identified with mysql_native_password by '%s'", username, host, password)
	_, err := conn.ExecuteFetch(sql, 1000, false)
	return err
}

func DropUser(conn *mysql.Conn, username, host string) {
	conn.ExecuteFetch(fmt.Sprintf("DROP USER '%s'@'%s';", username, host), 1000, false)
}

func DropDatabase(conn *mysql.Conn, dbName string) {
	conn.ExecuteFetch(fmt.Sprintf("DROP DATABASE %v", dbName), 1000, false)

}

func TestReaderPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	globalUser := "TestSelectPriv_Global"
	DBUser := "TestSelectPriv_DB"
	TableUser := "TestSelectPriv_Table"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	err := CreateUser(conn, globalUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, DBUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, TableUser, host, password)
	require.Nil(t, err, "%v", err)
	InitTable(conn)
	defer func() {
		DropUser(conn, globalUser, host)
		DropUser(conn, DBUser, host)
		DropUser(conn, TableUser, host)
		DropDatabase(conn, "test")
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	DbVtParams := CreateVtParam(globalUser, host, password)
	globalUserConn, err := mysql.Connect(ctx, &DbVtParams)
	require.Nil(t, err)
	DBVtParams := CreateVtParam(DBUser, host, password)
	DBUserConn, err := mysql.Connect(ctx, &DBVtParams)
	require.Nil(t, err)
	TableVtParams := CreateVtParam(TableUser, host, password)
	TableUserConn, err := mysql.Connect(ctx, &TableVtParams)
	require.Nil(t, err)
	_, err = globalUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)

	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON test.* TO '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON test.t1 TO '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	_, err = globalUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NoError(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NoError(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NoError(t, err, "%v", err)

	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT ON *.* FROM '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT ON test.* FROM '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT ON test.t1 FROM '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	_, err = globalUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.Error(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.Error(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.Error(t, err, "%v", err)

}
func TestWriterPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	globalUser := "TestWriterPriv_Global"
	DBUser := "TestWriterPriv_DB"
	TableUser := "TestWriterPriv_Table"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	err := CreateUser(conn, globalUser, host, password)
	require.NoError(t, err, "%v", err)
	err = CreateUser(conn, DBUser, host, password)
	require.NoError(t, err, "%v", err)
	err = CreateUser(conn, TableUser, host, password)
	require.NoError(t, err, "%v", err)
	InitTable(conn)
	defer func() {
		DropUser(conn, globalUser, host)
		DropUser(conn, DBUser, host)
		DropUser(conn, TableUser, host)
		DropDatabase(conn, "test")
	}()
	// waiting vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	DbVtParams := CreateVtParam(globalUser, host, password)
	globalUserConn, err := mysql.Connect(ctx, &DbVtParams)
	require.NoError(t, err)
	DBVtParams := CreateVtParam(DBUser, host, password)
	DBUserConn, err := mysql.Connect(ctx, &DBVtParams)
	require.NoError(t, err)
	TableVtParams := CreateVtParam(TableUser, host, password)
	TableUserConn, err := mysql.Connect(ctx, &TableVtParams)
	require.NoError(t, err)
	performDatabaseOperations := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
	}
	performDatabaseOperations(globalUserConn)
	performDatabaseOperations(DBUserConn)
	performDatabaseOperations(TableUserConn)

	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT,INSERT,DELETE,UPDATE ON *.* TO '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT,INSERT,DELETE,UPDATE ON test.* TO '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT,INSERT,DELETE,UPDATE ON test.t1 TO '%s'@'%s'", TableUser, host), 1000, false)

	//waiting tablet pull data into memory from mysql
	time.Sleep(3 * time.Second)
	validateDatabaseOperationsNil := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
		require.NoError(t, err, "%v", err)
	}

	validateDatabaseOperationsNil(globalUserConn)
	validateDatabaseOperationsNil(DBUserConn)
	validateDatabaseOperationsNil(TableUserConn)

	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT,INSERT,DELETE,UPDATE ON *.* FROM '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT,INSERT,DELETE,UPDATE ON test.* FROM '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE SELECT,INSERT,DELETE,UPDATE ON test.t1 FROM '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	validateDatabaseOperationsNotNil := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
	}
	validateDatabaseOperationsNotNil(globalUserConn)
	validateDatabaseOperationsNotNil(DBUserConn)
	validateDatabaseOperationsNotNil(TableUserConn)
}
func TestAdminPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	globalUser := "TestAdminPriv_Global"
	DBUser := "TestAdminPriv_DB"
	TableUser := "TestAdminPriv_Table"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	err := CreateUser(conn, globalUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, DBUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, TableUser, host, password)
	require.Nil(t, err, "%v", err)
	InitTable(conn)

	defer func() {
		DropUser(conn, globalUser, host)
		DropUser(conn, DBUser, host)
		DropUser(conn, TableUser, host)
		DropDatabase(conn, "test")
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	DbVtParams := CreateVtParam(globalUser, host, password)
	globalUserConn, err := mysql.Connect(ctx, &DbVtParams)
	require.Nil(t, err)
	DBVtParams := CreateVtParam(DBUser, host, password)
	DBUserConn, err := mysql.Connect(ctx, &DBVtParams)
	require.Nil(t, err)
	TableVtParams := CreateVtParam(TableUser, host, password)
	TableUserConn, err := mysql.Connect(ctx, &TableVtParams)
	require.Nil(t, err)
	_, err = globalUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)

	conn.ExecuteFetch(fmt.Sprintf("GRANT SUPER ON *.* TO '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT ALL PRIVILEGES ON test.* TO '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT ALL PRIVILEGES ON test.t1 TO '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	_, err = globalUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.Nil(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v4 INT;", 1000, false)
	require.Nil(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v5 INT;", 1000, false)
	require.Nil(t, err, "%v", err)

	conn.ExecuteFetch(fmt.Sprintf("REVOKE SUPER ON *.* FROM '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE ALL PRIVILEGES ON test.* FROM '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("REVOKE ALL PRIVILEGES ON test.t1 FROM '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	_, err = globalUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v4 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v5 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
}
func TestOnlyReaderPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	globalUser := "TestSelectPriv_Global"
	DBUser := "TestSelectPriv_DB"
	TableUser := "TestSelectPriv_Table"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	err := CreateUser(conn, globalUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, DBUser, host, password)
	require.Nil(t, err, "%v", err)
	err = CreateUser(conn, TableUser, host, password)
	require.Nil(t, err, "%v", err)
	InitTable(conn)
	defer func() {
		DropUser(conn, globalUser, host)
		DropUser(conn, DBUser, host)
		DropUser(conn, TableUser, host)
		DropDatabase(conn, "test")
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	DbVtParams := CreateVtParam(globalUser, host, password)
	globalUserConn, err := mysql.Connect(ctx, &DbVtParams)
	require.Nil(t, err)
	DBVtParams := CreateVtParam(DBUser, host, password)
	DBUserConn, err := mysql.Connect(ctx, &DBVtParams)
	require.Nil(t, err)
	TableVtParams := CreateVtParam(TableUser, host, password)
	TableUserConn, err := mysql.Connect(ctx, &TableVtParams)
	require.Nil(t, err)
	_, err = globalUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = DBUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = TableUserConn.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)

	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON test.* TO '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON test.t1 TO '%s'@'%s'", TableUser, host), 1000, false)

	time.Sleep(3 * time.Second)
	validateOnlyReader := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("SELECT * from test.t1", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("insert into test.t1 values (null, 100)", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT", 1000, false)
		require.Error(t, err, "%v", err)
	}
	validateOnlyReader(globalUserConn)
	validateOnlyReader(DBUserConn)
	validateOnlyReader(TableUserConn)

}

func TestOnlyWriterPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	globalUser := "TestWriterPriv_Global"
	DBUser := "TestWriterPriv_DB"
	TableUser := "TestWriterPriv_Table"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	err := CreateUser(conn, globalUser, host, password)
	require.NoError(t, err, "%v", err)
	err = CreateUser(conn, DBUser, host, password)
	require.NoError(t, err, "%v", err)
	err = CreateUser(conn, TableUser, host, password)
	require.NoError(t, err, "%v", err)
	InitTable(conn)
	defer func() {
		DropUser(conn, globalUser, host)
		DropUser(conn, DBUser, host)
		DropUser(conn, TableUser, host)
		DropDatabase(conn, "test")
	}()
	// waiting vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	DbVtParams := CreateVtParam(globalUser, host, password)
	globalUserConn, err := mysql.Connect(ctx, &DbVtParams)
	require.NoError(t, err)
	DBVtParams := CreateVtParam(DBUser, host, password)
	DBUserConn, err := mysql.Connect(ctx, &DBVtParams)
	require.NoError(t, err)
	TableVtParams := CreateVtParam(TableUser, host, password)
	TableUserConn, err := mysql.Connect(ctx, &TableVtParams)
	require.NoError(t, err)
	performDatabaseOperations := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
		require.Error(t, err, "%v", err)
	}
	performDatabaseOperations(globalUserConn)
	performDatabaseOperations(DBUserConn)
	performDatabaseOperations(TableUserConn)

	conn.ExecuteFetch(fmt.Sprintf("GRANT INSERT,DELETE,UPDATE ON *.* TO '%s'@'%s'", globalUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT INSERT,DELETE,UPDATE ON test.* TO '%s'@'%s'", DBUser, host), 1000, false)
	conn.ExecuteFetch(fmt.Sprintf("GRANT INSERT,DELETE,UPDATE ON test.t1 TO '%s'@'%s'", TableUser, host), 1000, false)

	//waiting tablet pull data into memory from mysql
	time.Sleep(3 * time.Second)
	validateOnlyWriterPriv := func(conn *mysql.Conn) {
		_, err = conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
		require.NoError(t, err, "%v", err)
		_, err = conn.ExecuteFetch("Select * from test.t1", 1000, false)
		require.Error(t, err, "%v", err)
	}

	validateOnlyWriterPriv(globalUserConn)
	validateOnlyWriterPriv(DBUserConn)
	validateOnlyWriterPriv(TableUserConn)

}
