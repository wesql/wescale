package queries

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestPutFailPoint(t *testing.T) {
	execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		qr := utils.Exec(t, conn, "show failpoints")
		isExist := false
		for _, row := range qr.Rows {
			if row[0].ToString() == "TestFailPointError" {
				isExist = true
				require.Equal(t, "false", row[1].ToString(), "TestFailPointError should be false")
			}
		}
		require.True(t, isExist, "TestFailPointError is not exist")
		qr = utils.Exec(t, conn, "set @put_failpoint='TestFailPointError=return(1)'")
		for _, row := range qr.Rows {
			if row[0].ToString() == "TestFailPointError" {
				require.Equal(t, "true", row[1].ToString(), "testFailPoint should be false")
			}
		}
	})
}
func TestRemoveFailPoint(t *testing.T) {
	execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		qr := utils.Exec(t, conn, "show failpoints")
		isExist := false
		for _, row := range qr.Rows {
			if row[0].ToString() == "TestFailPointError" {
				isExist = true
			}
		}
		require.True(t, isExist, "TestFailPointError is not exist")
		qr = utils.Exec(t, conn, "set @put_failpoint='TestFailPointError=return(1)'")
		for _, row := range qr.Rows {
			if row[0].ToString() == "TestFailPointError" {
				require.Equal(t, "true", row[1].ToString(), "testFailPoint should be false")
			}
		}
		qr = utils.Exec(t, conn, "set @remove_failpoint='TestFailPointError'")
		for _, row := range qr.Rows {
			if row[0].ToString() == "TestFailPointError" {
				require.Equal(t, "false", row[1].ToString(), "testFailPoint should be false")
			}
		}
	})
}
