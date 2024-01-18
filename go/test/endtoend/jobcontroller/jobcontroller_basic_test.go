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
	t.Run("double int pk", doubleIntPK)
	t.Run("dateTime and int pk", datetimeAndIntPK)
	t.Run("complex where condition", complexWhereCondition)
	t.Run("subquery in where condition", subqueryInWhereCondition)
	t.Run("subquery in set", subqueryInSet)
	t.Run("subquery in where exist", subqueryInWhereExist)
	t.Run("foreign subquery inw where", foreignSubqueryInWhere)
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

	// rows has not been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and age > 50;`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows-50-1), rowsNotUpdated)

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
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
}

func doubleIntPK(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: UserTableSchema,
	}

	// create table
	tableName := "mytable"
	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf(
		`create table if not exists %s (
					id1 int ,
					id2 int,
					name varchar(256) not null,
					age int,
    				primary key(id1,id2)
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (id1, id2, name, age) values (%%a,%%a,%%a, %%a);`, tableName)
	totalRows := 10000
	for i := 0; i < totalRows/100; i++ {
		for j := 0; j < 100; j++ {
			insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
				sqltypes.Int64BindVariable(int64(i)),
				sqltypes.Int64BindVariable(int64(j)),
				sqltypes.StringBindVariable(fmt.Sprintf("name_%d_%d", i, j)),
				sqltypes.Int64BindVariable(int64(i*100+j)))
			require.NoError(t, err)
			query, err := VtgateExecQuery(t, &vtParams, insertDataSQL)
			require.NoError(t, err)
			require.Equal(t, 1, int(query.RowsAffected))
		}
	}

	query, err := VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
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

func datetimeAndIntPK(t *testing.T) {
	defer cluster.PanicHandler(t)
	vtParams := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: UserTableSchema,
	}

	// create table
	tableName := "mytable"
	_, err := VtgateExecQuery(t, &vtParams, fmt.Sprintf(
		`create table if not exists %s (
					id1 datetime ,
					id2 int,
					name varchar(256) not null,
					age int,
    				primary key(id1,id2)
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (id1, id2, name, age) values (%%a,%%a,%%a, %%a);`, tableName)
	totalRows := 10000
	currentTime := time.Now()
	for i := 0; i < totalRows/100; i++ {
		dateTime := currentTime.Add(time.Duration(i*24) * time.Hour).Format(time.DateTime)
		for j := 0; j < 100; j++ {
			insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
				sqltypes.StringBindVariable(dateTime),
				sqltypes.Int64BindVariable(int64(j)),
				sqltypes.StringBindVariable(fmt.Sprintf("name_%d_%d", i, j)),
				sqltypes.Int64BindVariable(int64(i*100+j)))
			require.NoError(t, err)
			query, err := VtgateExecQuery(t, &vtParams, insertDataSQL)
			require.NoError(t, err)
			require.Equal(t, 1, int(query.RowsAffected))
		}
	}

	query, err := VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)

	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and age > 50;`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows-50-1), rowsNotUpdated)

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
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
}

func complexWhereCondition(t *testing.T) {
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
    				c1 varchar(32),
    				c2 int,
    				c3 varchar(32),
    				c4 varchar(32),
    				c5 int default null,
    				c6 int
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (name,c1,c2,c3,c4,c6) values (%%a, %%a, %%a, %%a, %%a, %%a);`, tableName)
	totalRows := 10000
	for i := 0; i < totalRows; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)),
			sqltypes.StringBindVariable("v1"),
			sqltypes.Int64BindVariable(int64(101)),
			sqltypes.StringBindVariable("abcdefg"),
			sqltypes.StringBindVariable("a"),
			sqltypes.Int64BindVariable(int64(50)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	query, err = VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)

	// all rows has not been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123';`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows), rowsNotUpdated)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams,
		fmt.Sprintf(`update /*vt+ dml_split=true */ %s set name='123' where 
								(c1 = 'v1' or c2 > 100) and 
							    (c3 like 'abc%%' and c4 in ('a', 'b', 'c')) or 
                                c5 is null and c6 between 10 and 100;`, tableName))
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted\n", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all rows has been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123';`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
}

func subqueryInWhereCondition(t *testing.T) {
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
					c2 int
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// create table t2
	_, err = VtgateExecQuery(t, &vtParams,
		`create table if not exists t2 (
					id int auto_increment primary key,
					c2 int
	)`)
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, "t2"))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (name,c2) values (%%a,%%a);`, tableName)
	totalRows := 10000
	for i := 0; i < totalRows; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)),
			sqltypes.Int64BindVariable(int64(1)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	query, err = VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)

	// insert data into t2
	query, err = VtgateExecQuery(t, &vtParams, "insert into t2 (c2) values (1);")
	require.NoError(t, err)
	require.Equal(t, 1, int(query.RowsAffected))

	// all row should not be updated yet
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123 and c2 in (select c2 from t2)'`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows), rowsNotUpdated)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update /*vt+ dml_split=true */ %s set name='123' where c2 in (select c2 from t2) ;", tableName))
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all the table rows satisfy the where condition have been updated
	// all rows should be updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and c2 in (select c2 from t2)`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, "t2"))
	require.Nil(t, err)
}

func subqueryInSet(t *testing.T) {
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

	// create t2
	_, err = VtgateExecQuery(t, &vtParams,
		`create table if not exists t2 (
					id int auto_increment primary key,
					c1 varchar(256) not null
	)`)
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

	// insert data into t2
	query, err = VtgateExecQuery(t, &vtParams, "insert into t2 (c1) values ('123');")
	require.NoError(t, err)
	require.Equal(t, 1, int(query.RowsAffected))

	// rows has not been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and age > 50;`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows-50-1), rowsNotUpdated)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update /*vt+ dml_split=true */ %s set name = (select c1 from t2 limit 1) where age > 50;", tableName))
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all the table rows satisfy the where condition have been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and age > 50;`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, "t2"))
	require.Nil(t, err)
}

func subqueryInWhereExist(t *testing.T) {
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
					name varchar(256) not null
	)`, tableName))
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// create t2
	_, err = VtgateExecQuery(t, &vtParams,
		`create table if not exists t2 (
					id int auto_increment primary key,
					c2 varchar(256) not null
	)`)
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, tableName))

	// insert some data into table
	insertDataSQLTemplate := fmt.Sprintf(`insert into %s (name) values (%%a);`, tableName)
	totalRows := 10000
	for i := 0; i < totalRows; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	query, err = VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)

	// insert data into t2
	insertDataSQLTemplate = fmt.Sprintf(`insert into %s (c2) values (%%a);`, "t2")
	for i := 0; i < 500; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind(insertDataSQLTemplate,
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	// rows has not been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123';`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows), rowsNotUpdated)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("update /*vt+ dml_split=true */ %s set name = '123' where exists (select c2 from t2 where t2.c2 = mytable.name);", tableName))
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all the table rows satisfy the where condition have been updated
	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123' and exists (select c2 from t2 where t2.c2 = mytable.name);`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("rows not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	query, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf(`select count(*) as cnt from %s where name!='123'`, tableName))
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("rows which name not equal '123' %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows-500), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, tableName))
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.%s", UserTableSchema, "t2"))
	require.Nil(t, err)
}

func foreignSubqueryInWhere(t *testing.T) {
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
	_, err = VtgateExecQuery(t, &vtParams,
		`create table if not exists mytable (
					id int auto_increment primary key,
					name varchar(256) not null
	)`)
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, "mytable"))

	// create t2
	_, err = VtgateExecQuery(t, &vtParams,
		`create table if not exists t2 (
					id int auto_increment primary key,
					mytable_id int not null,
					foreign key (mytable_id) references mytable(id)
	)`)
	require.Nil(t, err)
	require.Equal(t, true, CheckTableExist(t, &vtParams, "t2"))

	// insert some data into table
	totalRows := 10000
	for i := 0; i < totalRows; i++ {
		insertDataSQL, err := sqlparser.ParseAndBind("insert into mytable (name) values (%a)",
			sqltypes.StringBindVariable(fmt.Sprintf("name_%d", i)))
		require.NoError(t, err)
		query, err = VtgateExecQuery(t, &vtParams, insertDataSQL)
		require.NoError(t, err)
		require.Equal(t, 1, int(query.RowsAffected))
	}

	query, err = VtgateExecQuery(t, &vtParams, "select count(*) as cnt from mytable;")
	require.Nil(t, err)
	row, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row inserted is %d\n", row)
	require.Equal(t, int64(totalRows), row)

	// insert data into t2
	query, err = VtgateExecQuery(t, &vtParams, "insert into t2(mytable_id) select id from mytable where mytable.id>1000;")
	require.NoError(t, err)
	require.Equal(t, 9000, int(query.RowsAffected))

	// rows has not been updated
	query, err = VtgateExecQuery(t, &vtParams, `select count(*) as cnt from mytable where name!='123';`)
	require.Nil(t, err)
	rowsNotUpdated, _ := query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("row not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(totalRows), rowsNotUpdated)

	// submit a DML job
	query, err = VtgateExecQuery(t, &vtParams, "update /*vt+ dml_split=true */ mytable set name = '123' where id = (select mytable_id from t2 where t2.mytable_id = mytable.id);")
	require.Nil(t, err)
	jobUUID := query.Named().Rows[0]["job_uuid"].ToString()
	fmt.Printf("job %s submitted", jobUUID)

	// wait for job to complete
	require.Equal(t, jobcontroller.CompletedStatus, WaitForJobStatus(t, &vtParams, jobUUID, 10*time.Minute, jobcontroller.CompletedStatus))

	// verify that all the table rows satisfy the where condition have been updated
	query, err = VtgateExecQuery(t, &vtParams, `select count(*) as cnt from mytable where name!='123' and id > (select min(mytable_id) from t2 where t2.mytable_id = mytable.id);`)
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("rows not updated is %d\n", rowsNotUpdated)
	require.Equal(t, int64(0), rowsNotUpdated)

	query, err = VtgateExecQuery(t, &vtParams, `select count(*) as cnt from mytable where name!='123'`)
	require.Nil(t, err)
	rowsNotUpdated, _ = query.Named().Rows[0]["cnt"].ToInt64()
	fmt.Printf("rows which name not equal '123' %d\n", rowsNotUpdated)
	require.Equal(t, int64(1000), rowsNotUpdated)

	// drop table
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.t2", UserTableSchema))
	require.Nil(t, err)
	_, err = VtgateExecQuery(t, &vtParams, fmt.Sprintf("drop table if exists %s.mytable", UserTableSchema))
	require.Nil(t, err)
}
