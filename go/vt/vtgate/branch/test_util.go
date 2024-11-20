package branch

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func NewMockMysqlService(t *testing.T) (*MysqlService, sqlmock.Sqlmock) {
	// use QueryMatcherEqual to match exact query
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))

	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}

	service, err := NewMysqlService(db)
	if err != nil {
		t.Fatalf("failed to create mysql service: %v", err)
	}

	return service, mock
}

// todo optimize this
func addMockShowCreateTable(mock sqlmock.Sqlmock, db, table, createTableStmt string) {
	mock.ExpectQuery("SHOW CREATE TABLE " + db + "." + table).
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(table, createTableStmt))
}

var tableInfos = []TableInfo{
	{database: "db1", name: "table1"},
	{database: "db1", name: "table2"},
	{database: "db2", name: "table3"},
	{database: "db3", name: "table4"},
}

func InitMockTableInfos(mock sqlmock.Sqlmock) {
	query1 := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
	rows1 := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for _, tableInfo := range tableInfos {
		rows1 = rows1.AddRow(tableInfo.database, tableInfo.name)
	}
	mock.ExpectQuery(query1).WillReturnRows(rows1)

	query2 := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('db1')"
	rows2 := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for _, tableInfo := range tableInfos {
		if tableInfo.database == "db1" {
			continue
		}
		rows2 = rows2.AddRow(tableInfo.database, tableInfo.name)
	}
	mock.ExpectQuery(query2).WillReturnRows(rows2)
}
