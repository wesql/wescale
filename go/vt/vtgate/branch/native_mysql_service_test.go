package branch

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMysqlService(t *testing.T) {
	tests := []struct {
		name    string
		mockFn  func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successful connection",
			mockFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing()
			},
			wantErr: false,
		},
		{
			name: "ping failed",
			mockFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectPing().WillReturnError(errors.New("ping failed"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			tt.mockFn(mock)

			service, err := NewMysqlService(db)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, service)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, service)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestNewMysqlServiceWithConfig(t *testing.T) {
	config := &mysql.Config{
		User: "invalid",
		Net:  "invalid",
	}

	service, err := NewMysqlServiceWithConfig(config)
	assert.Error(t, err)
	assert.Nil(t, service)
}

func TestQuery(t *testing.T) {
	service, mock := NewMockMysqlService(t)
	defer service.Close()

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	_, err := service.Query("SELECT 1")
	assert.NoError(t, err)

	mock.ExpectQuery("INSERT * FROM t1").WillReturnError(errors.New("synx error"))
	_, err = service.Query("SELECT 1")
	assert.Error(t, err)
}

func TestClose(t *testing.T) {
	service, _ := NewMockMysqlService(t)
	service.Close()
	_, err := service.Query("SELECT 1")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestExecuteInTxn(t *testing.T) {
	service, mock := NewMockMysqlService(t)
	defer service.Close()

	t.Run("Successful Transaction", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec("QUERY_1").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("QUERY_2").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := service.ExecuteInTxn("QUERY_1", "QUERY_2")
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Failed Transaction", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec("QUERY_1").WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		err := service.ExecuteInTxn("QUERY_1", "QUERY_2")
		assert.Error(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

//func TestTableSchemaEqual(t *testing.T) {
//	tests := []struct {
//		Name          string
//		table1        string
//		table2        string
//		hints         *schemadiff.DiffHints
//		expectEqual   bool
//		expectMessage string
//		expectError   bool
//	}{
//		{
//			Name:   "identical schemas",
//			table1: "CREATE TABLE b3 (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
//
//			table2: "CREATE TABLE b3 (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
//
//			hints:         &schemadiff.DiffHints{TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways},
//			expectEqual:   true,
//			expectMessage: "",
//			expectError:   false,
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.Name, func(t *testing.T) {
//			schema1, err := sqlparser.Parse(tt.table1)
//			assert.NoError(t, err)
//			createTable1 := schema1.(*sqlparser.CreateTable)
//			entity1 := &schemadiff.CreateTableEntity{CreateTable: createTable1}
//
//			schema2, err := sqlparser.Parse(tt.table2)
//			assert.NoError(t, err)
//			createTable2 := schema2.(*sqlparser.CreateTable)
//			entity2 := &schemadiff.CreateTableEntity{CreateTable: createTable2}
//
//			equal, message, err := tableSchemaEqual(entity1, entity2, tt.hints)
//
//			if tt.expectError {
//				assert.Error(t, err)
//				return
//			}
//
//			assert.NoError(t, err)
//			assert.Equal(t, tt.expectEqual, equal)
//
//			if tt.expectMessage != "" {
//				assert.Contains(t, message, tt.expectMessage)
//			} else {
//				assert.Empty(t, message)
//			}
//		})
//	}
//}
