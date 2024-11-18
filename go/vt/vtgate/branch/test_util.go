package branch

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMockMysqlService(t *testing.T) (*MysqlService, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
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
