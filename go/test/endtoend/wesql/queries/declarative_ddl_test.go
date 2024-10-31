package queries

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestDeclarativeDDL(t *testing.T) {
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {
		_, err := conn.ExecuteFetch("set @@enable_declarative_ddl=true;", 1000, true)
		assert.Nil(t, err)
		// return error when darabase not exist
		_, err = conn.ExecuteFetch("create table dddb.t1(id int primary key);", 1000, true)
		assert.NotNil(t, err)
		_, err = conn.ExecuteFetch("create database dddb;", 1000, true)
		assert.Nil(t, err)
	})

	execWithConn(t, "mysql", func(conn *mysql.Conn) {
		_, err := conn.ExecuteFetch("set @@enable_declarative_ddl=true;", 1000, true)
		assert.Nil(t, err)
		// create new table
		_, err = conn.ExecuteFetch("set @@ddl_strategy='direct';\n", 1000, true)
		assert.Nil(t, err)
		_, err = conn.ExecuteFetch("create table dddb.t1(id int primary key);", 1000, true)
		assert.Nil(t, err)
		// declarative
		_, err = conn.ExecuteFetch("create table dddb.t1(id varchar(64) primary key, age int);", 1000, true)
		assert.Nil(t, err)
		checkTableColExist(t, conn, "dddb", "t1", "`id` varchar(64)")
		checkTableColExist(t, conn, "dddb", "t1", "`age` int")
		checkTableColNotExist(t, conn, "dddb", "t1", "`id` int")

		// online ddl
		_, err = conn.ExecuteFetch("set @@ddl_strategy='online'", 1000, true)
		assert.Nil(t, err)

		qr, err := conn.ExecuteFetch("create table dddb.t1(id int primary key, height int);\n", 1000, true)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(qr.Rows))
		uuid := qr.Named().Rows[0].AsString("uuid", "0")
		WaitForMigrationStatus(t, conn, uuid, 60*time.Second, schema.OnlineDDLStatusComplete)
		checkTableColExist(t, conn, "dddb", "t1", "`height` int")
		checkTableColExist(t, conn, "dddb", "t1", "`id` int")

		qr, err = conn.ExecuteFetch(`
			create table dddb.t1(
				id int primary key,
				content1 text,
				content2 text,
				fulltext idx_content1(content1),
				fulltext idx_content2(content2)
			);`, 1000, true)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(qr.Rows)) // Should return 2 UUIDs

		// Wait for both migrations to complete
		for _, row := range qr.Named().Rows {
			uuid := row.AsString("uuid", "0")
			WaitForMigrationStatus(t, conn, uuid, 60*time.Second, schema.OnlineDDLStatusComplete)
		}

		// Verify columns and indexes exist
		checkTableColExist(t, conn, "dddb", "t1", "`content1` text")
		checkTableColExist(t, conn, "dddb", "t1", "`content2` text")

		// Verify fulltext indexes exist
		qr, err = conn.ExecuteFetch("show index from dddb.t1 where Index_type = 'FULLTEXT';", 1000, true)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(qr.Rows))
	})
}

func getCreateTableStatement(t *testing.T, conn *mysql.Conn, dbName, tableName string) (statement string) {
	queryResult, err := conn.ExecuteFetch(fmt.Sprintf("show create table %s.%s;", dbName, tableName), 1000, true)
	require.Nil(t, err)
	assert.Equal(t, len(queryResult.Rows), 1)
	assert.GreaterOrEqual(t, len(queryResult.Rows[0]), 2) // table name, create statement, and if it's a view then additional columns
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func checkTableColExist(t *testing.T, conn *mysql.Conn, dbName, tableName, expectColumn string) {
	createStatement := getCreateTableStatement(t, conn, dbName, tableName)
	fmt.Printf("table create statement is %s\n", createStatement)
	assert.Contains(t, createStatement, expectColumn)

}

func checkTableColNotExist(t *testing.T, conn *mysql.Conn, dbName, tableName, expectColumn string) {
	createStatement := getCreateTableStatement(t, conn, dbName, tableName)
	fmt.Printf("table create statement is %s\n", createStatement)
	assert.NotContains(t, createStatement, expectColumn)
}

func WaitForMigrationStatus(t *testing.T, conn *mysql.Conn, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
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
		r, err := conn.ExecuteFetch(query, 1000, true)
		assert.Nil(t, err)
		for _, row := range r.Named().Rows {
			lastKnownStatus = row["migration_status"].ToString()
			message := row["message"].ToString()
			if lastKnownStatus == string(schema.OnlineDDLStatusFailed) {
				t.Logf("schemaMigration fail, message : %v", message)
			}
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				return schema.OnlineDDLStatus(lastKnownStatus)
			}
		}
		time.Sleep(1 * time.Second)
	}
	return schema.OnlineDDLStatus(lastKnownStatus)
}
