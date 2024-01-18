/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/jobcontroller"
)

const (
	UserTableSchema           = "testdb666"
	CreateSchemaSQL           = "create database if not exists %s"
	SingleIntPKCreatTeableSQL = ` create table if not exists %s (
     	id int auto_increment primary key,
     	name varchar(256) not null,
     	age int
 	);`
	SingleIntPKInsertDataSQL = `insert into %s (name, age) values (%%a, %%a);`
	SingleIntPKDMLSQL        = `update /*vt+ dml_split=true */ %s set name='123' where age>50;`
	SingleIntPKVerifySQL     = `select count(*) as cnt from %s where name!='123' and age > 50;`
	DropTableSQL             = "drop table if exists %s.%s"
)

// WaitForJobStatus waits for a job to reach either provided statuses (returns immediately), or eventually time out
func WaitForJobStatus(t *testing.T, vtParams *mysql.ConnParams, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) string {
	query, err := sqlparser.ParseAndBind("show dml_job %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			lastKnownStatus = row["status"].ToString()
			message := row["message"].ToString()
			if lastKnownStatus == string(jobcontroller.FailedStatus) {
				t.Logf("Job fail, message : %v", message)
			}
			if row["job_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				return lastKnownStatus
			}
		}

		time.Sleep(1 * time.Second)
	}
	return lastKnownStatus
}

// CheckTableExist checks the number of tables in the first two shards.
func CheckTableExist(t *testing.T, vtParams *mysql.ConnParams, tableName string) bool {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(fmt.Sprintf("show tables like '%s'", tableName), math.MaxInt64, true)
	require.Nil(t, err)
	return len(qr.Rows) > 0
}

func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string) (*sqltypes.Result, error) {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	return qr, err
}
