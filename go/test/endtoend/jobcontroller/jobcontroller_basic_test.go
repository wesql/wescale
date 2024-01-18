/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/jobcontroller"
)

func TestJobControllerBasic(t *testing.T) {
	t.Run("create user db", createUserDB)
	t.Run("single int pk", singleIntPK)
}

func createUserDB(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf("create database if not exists %s", UserTableSchema))
	require.Nil(t, err)
}

func singleIntPK(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: UserTableSchema,
	}

	query, err := VtgateExecQuery(t, &vtParams, "select version() as v")
	require.Nil(t, err)
	v, err := query.Named().Rows[0].ToString("v")
	require.Nil(t, err)
	fmt.Printf("version: %s\n", v)

	// create table
	tableName := "mytable"
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(
		`create table if not exists %s (
					id int auto_increment primary key,
					name varchar(256) not null,
					age int
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (name, age) values (%%a, %%a);`, tableName)
	totalRows := 10000
	for i := 0; i < totalRows; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)),
			sqltypes.Int64BindVariable(int64(i)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	query, err = VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update /*vt+ dml_split=true */ %s set name='123' where age > 50;", tableName))
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all the table rows satisfy the where condition have been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and age > 50;`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
}
