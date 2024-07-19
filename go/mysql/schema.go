/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file contains the mysql queries used by different parts of the code.

const (
	// BaseShowPrimary is the base query for fetching primary key info.
	//BaseShowPrimary = `
	//	SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
	//	FROM information_schema.STATISTICS
	//	WHERE TABLE_SCHEMA = DATABASE() AND LOWER(INDEX_NAME) = 'primary'
	//	ORDER BY table_name, SEQ_IN_INDEX`
	BaseShowPrimary = `
		SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name, TABLE_SCHEMA as table_schema
		FROM information_schema.STATISTICS
		WHERE LOWER(INDEX_NAME) = 'primary'
		ORDER BY table_name, SEQ_IN_INDEX`
	// BaseShowPrimaryOfTable is the base query for fetching primary key info.
	BaseShowPrimaryOfTable = `
		SELECT COLUMN_NAME as column_name
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = '%s' AND LOWER(INDEX_NAME) = 'primary' AND TABLE_NAME = '%s'
		ORDER BY SEQ_IN_INDEX`
	// ShowRowsRead is the query used to find the number of rows read.
	ShowRowsRead = "show status like 'Innodb_rows_read'"

	// DetectSchemaChange query detects if there is any schema change from previous copy.
	DetectSchemaChange = `
SELECT DISTINCT table_schema,table_name
FROM (
	SELECT table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM information_schema.columns

	UNION ALL

	SELECT table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM mysql.schemacopy

) _inner
GROUP BY table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
HAVING COUNT(*) = 1
`

	// DetectSchemaChangeOnlyBaseTable query detects if there is any schema change from previous copy excluding view tables.
	DetectSchemaChangeOnlyBaseTable = `
SELECT DISTINCT table_schema,table_name
FROM (
	SELECT table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM information_schema.columns
	WHERE table_name in (select table_name from information_schema.tables where table_type = 'BASE TABLE')

	UNION ALL

	SELECT table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
	FROM mysql.schemacopy
	
) _inner
GROUP BY table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
HAVING COUNT(*) = 1
`

	// ClearSchemaCopy query clears the schemacopy table.
	ClearSchemaCopy = `delete from mysql.schemacopy`

	// InsertIntoSchemaCopy query copies over the schema information from information_schema.columns table.
	InsertIntoSchemaCopy = `insert mysql.schemacopy
select table_schema, table_name, column_name, ordinal_position, character_set_name, collation_name, data_type, column_key
from information_schema.columns`

	// fetchColumns are the columns we fetch
	fetchColumns = "table_name, column_name, data_type, collation_name"

	// FetchUpdatedTables queries fetches all information about updated tables
	FetchUpdatedTables = `select  ` + fetchColumns + `
from mysql.schemacopy
where table_name in ::table_names and table_schema = :table_schema
order by table_name, ordinal_position`

	// FetchTables queries fetches all information about tables
	FetchTables = `select ` + fetchColumns + `
from mysql.schemacopy
where table_schema = :table_schema
order by table_name, ordinal_position`

	// GetColumnNamesQueryPatternForTable is used for mocking queries in unit tests
	GetColumnNamesQueryPatternForTable = `SELECT COLUMN_NAME.*TABLE_NAME.*%s.*`

	// Views
	InsertIntoViewsTable = `insert into mysql.views (
    table_schema,
	table_name,
	create_statement) values (:table_schema, :table_name, :create_statement)`

	ReplaceIntoViewsTable = `replace into mysql.views (
	table_schema,
	table_name,
	create_statement) values (:table_schema, :table_name, :create_statement)`

	UpdateViewsTable = `update mysql.views 
	set create_statement = :create_statement 
	where table_schema = :table_schema and table_name = :table_name`

	// DeleteFromViewsTableWithoutCondition the sql should be added pairs of table_schema and table_name before execution
	DeleteFromViewsTableWithoutCondition = `delete from mysql.views`

	SelectAllViews = `select concat(table_schema,'.',table_name), updated_at from mysql.views`

	// FetchUpdatedViews queries fetches information about updated views
	FetchUpdatedViews = `select table_name, create_statement from mysql.views where table_name in ::viewnames and table_schema = :table_schema`

	// FetchViews queries fetches all views
	FetchViews = `select table_name, create_statement from mysql.views where table_schema = :table_schema`

	FetchDbList = `select schema_name from information_schema.schemata`

	FetchUser = `select user,host,plugin,authentication_string from mysql.user`

	FetchGlobalPriv = `SELECT USER,HOST,SELECT_PRIV,INSERT_PRIV,UPDATE_PRIV,DELETE_PRIV,SUPER_PRIV from mysql.user`

	FetchTablePriv = `SELECT USER,HOST,DB,TABLE_NAME,TABLE_PRIV from mysql.tables_priv`

	FetchDataBasePriv = `SELECT USER,HOST,DB,SELECT_PRIV,INSERT_PRIV,UPDATE_PRIV,DELETE_PRIV,CREATE_PRIV,REFERENCES_PRIV,INDEX_PRIV,ALTER_PRIV,Create_tmp_table_priv,LOCK_TABLES_PRIV,CREATE_VIEW_PRIV,SHOW_VIEW_PRIV,Create_routine_priv,Alter_routine_priv,Execute_priv,Event_priv,Trigger_priv from mysql.db`

	FetchThreads = `SHOW status like '%Threads_%'`

	ThreadsCached = "Threads_cached"

	ThreadsConnected = "Threads_connected"

	ThreadsRunning = "Threads_running"

	ThreadsCreated = "Threads_created"
)

// BaseShowTablesFields contains the fields returned by a BaseShowTables or a BaseShowTablesForTable command.
// They are validated by the
// testBaseShowTables test.
var BaseShowTablesFields = []*querypb.Field{{
	Name:         "t.table_name",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_NAME",
	ColumnLength: 192,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "t.table_type",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_TYPE",
	ColumnLength: 192,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "unix_timestamp(t.create_time)",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "t.table_comment",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_COMMENT",
	ColumnLength: 6144,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
}, {
	Name:         "i.file_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "i.allocated_size",
	Type:         querypb.Type_INT64,
	ColumnLength: 11,
	Charset:      collations.CollationBinaryID,
	Flags:        uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_NUM_FLAG),
}, {
	Name:         "t.table_schema",
	Type:         querypb.Type_VARCHAR,
	Table:        "tables",
	OrgTable:     "TABLES",
	Database:     "information_schema",
	OrgName:      "TABLE_TYPE",
	ColumnLength: 192,
	Charset:      collations.CollationUtf8ID,
	Flags:        uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
},
}

// BaseShowTablesRow returns the fields from a BaseShowTables or
// BaseShowTablesForTable command.
func BaseShowTablesRow(tableName string, isView bool, comment string) []sqltypes.Value {
	tableType := "BASE TABLE"
	if isView {
		tableType = "VIEW"
	}
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableName)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableType)),
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325875")), // unix_timestamp(create_time)
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(comment)),
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("100")), // file_size
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("150")), // allocated_size
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("fakesqldb")),
	}
}

// ShowPrimaryFields contains the fields for a BaseShowPrimary.
var ShowPrimaryFields = []*querypb.Field{{
	Name: "table_name",
	Type: sqltypes.VarChar,
}, {
	Name: "column_name",
	Type: sqltypes.VarChar,
}, {
	Name: "table_schema",
	Type: sqltypes.VarChar,
}}

// ShowPrimaryRow returns a row for a primary key column.
func ShowPrimaryRow(tableName, colName string) []sqltypes.Value {
	return []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(tableName)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte(colName)),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("fakesqldb")),
	}
}
