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

func TestReaderPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	username := "TestSelectPriv"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	sql := fmt.Sprintf("CREATE USER '%s'@'%s' identified with mysql_native_password by '%s'", username, host, password)
	_, err := conn.ExecuteFetch(sql, 1000, false)
	require.Nil(t, err, "%v", err)
	conn.ExecuteFetch("CREATE DATABASE test", 1000, false)
	conn.ExecuteFetch("use test", 1000, false)
	conn.ExecuteFetch("CREATE TABLE t1 (v1 INT AUTO_INCREMENT PRIMARY KEY,  v2 INT);", 1000, false)
	conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
	defer func() {
		conn.ExecuteFetch(fmt.Sprintf("DROP USER '%s'@'%s';", username, host), 1000, false)
		conn.ExecuteFetch("DROP DATABASE test", 1000, false)
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:  host,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: username,
		Pass:  password,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	_, err = conn1.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.NotNil(t, err, "%v", err)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%s'", username, host), 1000, false)
	time.Sleep(3 * time.Second)
	qr, err := conn1.ExecuteFetch("SELECT * from test.t1", 1000, false)
	require.Nil(t, err, "%v", err)
	require.Equal(t, "1", qr.Rows[0][0].ToString())
	require.Equal(t, "100", qr.Rows[0][1].ToString())
}
func TestWriterPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	username := "TestWriterPriv"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	sql := fmt.Sprintf("CREATE USER '%s'@'%s' identified with mysql_native_password by '%s'", username, host, password)
	_, err := conn.ExecuteFetch(sql, 1000, false)
	require.Nil(t, err, "%v", err)
	conn.ExecuteFetch("CREATE DATABASE test", 1000, false)
	conn.ExecuteFetch("use test", 1000, false)
	conn.ExecuteFetch("CREATE TABLE t1 (v1 INT AUTO_INCREMENT PRIMARY KEY,  v2 INT);", 1000, false)
	conn.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
	defer func() {
		conn.ExecuteFetch(fmt.Sprintf("DROP USER '%s'@'%s';", username, host), 1000, false)
		conn.ExecuteFetch("DROP DATABASE test", 1000, false)
	}()
	// waiting vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:  host,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: username,
		Pass:  password,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	_, err = conn1.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = conn1.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
	require.NotNil(t, err, "%v", err)
	_, err = conn1.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
	require.NotNil(t, err, "%v", err)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SELECT,INSERT,DELETE,UPDATE ON *.* TO '%s'@'%s'", username, host), 1000, false)
	//waiting vtgate pull data into memory from mysql.user
	time.Sleep(3 * time.Second)
	_, err = conn1.ExecuteFetch("Insert into test.t1 values(null,100)", 1000, false)
	require.Nil(t, err, "%v", err)
	_, err = conn1.ExecuteFetch("Update test.t1 set v2=101 where v1=1", 1000, false)
	require.Nil(t, err, "%v", err)
	_, err = conn1.ExecuteFetch("Delete From test.t1  where v1=1", 1000, false)
	require.Nil(t, err, "%v", err)
}
func TestAdminPriv(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	username := "TestSelectPriv"
	password := "password"
	host := "127.0.0.1"
	// create user 'test_user1'@'localhost' by 'password'
	sql := fmt.Sprintf("CREATE USER '%s'@'%s' identified with mysql_native_password by '%s'", username, host, password)
	_, err := conn.ExecuteFetch(sql, 1000, false)
	require.Nil(t, err, "%v", err)
	conn.ExecuteFetch("CREATE DATABASE test", 1000, false)
	conn.ExecuteFetch("use test", 1000, false)
	conn.ExecuteFetch("CREATE TABLE t1 (v1 INT AUTO_INCREMENT PRIMARY KEY,  v2 INT);", 1000, false)
	defer func() {
		conn.ExecuteFetch(fmt.Sprintf("DROP USER '%s'@'%s';", username, host), 1000, false)
		conn.ExecuteFetch("DROP DATABASE test", 1000, false)
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:  host,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: username,
		Pass:  password,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	_, err = conn1.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.NotNil(t, err, "%v", err)
	conn.ExecuteFetch(fmt.Sprintf("GRANT SUPER ON *.* TO '%s'@'%s'", username, host), 1000, false)
	time.Sleep(3 * time.Second)
	_, err = conn1.ExecuteFetch("ALTER TABLE test.t1 ADD COLUMN v3 INT;", 1000, false)
	require.Nil(t, err, "%v", err)
}
