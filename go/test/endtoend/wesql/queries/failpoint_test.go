package queries

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestFailPoint(t *testing.T) {
	execWithConnByVtgate(t, DefaultKeyspaceName, 1, func(conn *mysql.Conn) {
		qr := utils.Exec(t, conn, "show failpoint")
		isExist := false
		for _, row := range qr.Rows {
			if row[0].ToString() == "testFailPoint" {
				isExist = true
				require.Equal(t, "false", row[1].ToString(), "testFailPoint should be false")
			}
		}
		require.True(t, isExist, "testFailPoint is not exist")
		qr = utils.Exec(t, conn, "set @put_failpoint='testFailPoint=return(1)'")
		for _, row := range qr.Rows {
			if row[0].ToString() == "testFailPoint" {
				isExist = true
				require.Equal(t, "true", row[1].ToString(), "testFailPoint should be false")
			}
		}
	})
}
