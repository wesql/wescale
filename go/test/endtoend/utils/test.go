/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package utils

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestDatabaseExists(t testing.TB, conn *mysql.Conn, dbName string) bool {
	t.Helper()
	qr := Exec(t, conn, fmt.Sprintf("show databases like '%s'", dbName))
	return len(qr.Rows) == 1
}
