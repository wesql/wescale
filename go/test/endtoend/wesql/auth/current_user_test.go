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

	"github.com/wesql/wescale/go/mysql"
)

func TestCurrentUserFunc(t *testing.T) {
	conn := getBackendPrimaryMysqlConn()
	ctx := context.Background()
	username := "test_current_user"
	password := "password"
	host := "127.0.0.1"
	errPassword := "err"
	// create user 'test_user1'@'localhost' by 'password'
	sql := fmt.Sprintf("CREATE USER '%s'@'%s' identified with mysql_native_password by '%s'", username, host, password)
	_, err := conn.ExecuteFetch(sql, 1000, false)
	if err != nil {
		t.Logf("%v", err)
	}
	defer func() {
		conn.ExecuteFetch(fmt.Sprintf("DROP USER '%s'@'%s';", username, host), 1000, false)
	}()
	// wait vtgate pull user from mysql.user
	time.Sleep(4 * time.Second)
	conn.ExecuteFetch("SELECT user,host,plugin from mysql.user", 1000, false)
	vtParams := mysql.ConnParams{
		Host:  host,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: username,
		Pass:  password,
	}
	conn, err = mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	vtParams.Pass = errPassword
	_, err = mysql.Connect(ctx, &vtParams)
	require.NotNil(t, err)
	qr, err := conn.ExecuteFetch("SELECT current_user()", 1000, false)
	require.Nil(t, err, "err: %v", err)
	require.Equal(t, fmt.Sprintf("%s@%s", username, host), qr.Rows[0][0].ToString())
}
