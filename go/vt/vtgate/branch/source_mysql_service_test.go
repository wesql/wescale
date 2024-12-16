package branch

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTableSchemaOneByOne(t *testing.T) {
	service, mock := NewMockMysqlService(t)
	s := NewSourceMySQLService(service)

	InitMockShowCreateTable(mock)
	tableInfo := make([]TableInfo, 0)
	for d, tables := range BranchSchemaForTest.branchSchema {
		for table, _ := range tables {
			tableInfo = append(tableInfo, TableInfo{
				database: d,
				name:     table,
			})
		}
	}

	// get one table schema each time, because it's more convenient for mock
	got, err := s.getTableSchemaOneByOne(tableInfo)
	assert.Nil(t, err)
	compareBranchSchema(t, BranchSchemaForTest, got)
}

func compareBranchSchema(t *testing.T, want, got *BranchSchema) {
	if want == nil && got == nil {
		return
	}

	if want == nil || got == nil {
		assert.Equal(t, want, got, "One of the BranchSchema is nil")
		return
	}

	// Compare the outer map lengths
	assert.Equal(t, len(want.branchSchema), len(got.branchSchema), "schema databases length mismatch")

	// Compare each database's tables
	for dbName, wantTables := range want.branchSchema {
		gotTables, exists := got.branchSchema[dbName]
		assert.True(t, exists, "missing database %s in schema", dbName)
		if !exists {
			continue
		}

		// Compare tables map lengths for this database
		assert.Equal(t, len(wantTables), len(gotTables),
			"tables length mismatch for database %s", dbName)

		// Compare each table's create statement
		for tableName, wantCreate := range wantTables {
			gotCreate, exists := gotTables[tableName]
			assert.True(t, exists, "missing table %s in database %s", tableName, dbName)
			if exists {
				// Compare the create table statements
				assert.Equal(t, wantCreate, gotCreate,
					"create table statement mismatch for table %s in database %s",
					tableName, dbName)
			}
		}
	}
}

func Test_buildTableInfosQuerySQL(t *testing.T) {
	tests := []struct {
		name            string
		databaseInclude []string
		databaseExclude []string
		startSchema     string
		startTable      string
		wantSQL         string
		wantErr         bool
		errMsg          string
	}{
		{
			name:            "Normal case with include and exclude",
			databaseInclude: []string{"db1", "db2"},
			databaseExclude: []string{"test1", "test2"},
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND TABLE_SCHEMA IN ('db1','db2') AND TABLE_SCHEMA NOT IN ('test1','test2') "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
		{
			name:            "Normal case with include and exclude",
			databaseInclude: []string{"db1", "db2", "", ""},
			databaseExclude: []string{"test1", "test2", "", ""},
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND TABLE_SCHEMA IN ('db1','db2') AND TABLE_SCHEMA NOT IN ('test1','test2') "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
		{
			name:            "Only include databases",
			databaseInclude: []string{"db1", "db2"},
			databaseExclude: nil,
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND TABLE_SCHEMA IN ('db1','db2') "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
		{
			name:            "Only exclude databases",
			databaseInclude: nil,
			databaseExclude: []string{"test1", "test2"},
			wantSQL:         "",
			wantErr:         true,
		},
		{
			name:            "Empty include and exclude",
			databaseInclude: nil,
			databaseExclude: nil,
			wantSQL:         "",
			wantErr:         true,
		},
		{
			name:            "Include with wildcard",
			databaseInclude: []string{"db1", "*", "db2"},
			databaseExclude: []string{"test1"},
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND TABLE_SCHEMA NOT IN ('test1') "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
		{
			name:            "Exclude with wildcard should error",
			databaseInclude: []string{"db1", "db2"},
			databaseExclude: []string{"test1", "*", "test2"},
			wantSQL:         "",
			wantErr:         true,
			errMsg:          "exclude all databases is not supported",
		},
		{
			name:            "Empty strings in include",
			databaseInclude: []string{""},
			databaseExclude: nil,
			wantSQL:         "",
			wantErr:         true,
		},
		{
			name:            "Empty strings in include",
			databaseInclude: []string{"", ""},
			databaseExclude: nil,
			wantSQL:         "",
			wantErr:         true,
		},
		{
			name:            "Empty strings in exclude",
			databaseInclude: []string{"*"},
			databaseExclude: []string{""},
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
		{
			name:            "Empty strings in exclude",
			databaseInclude: nil,
			databaseExclude: []string{""},
			wantSQL:         "",
			wantErr:         true,
		},
		{
			name:            "Special characters in database names",
			databaseInclude: []string{"db-1", "db_2"},
			databaseExclude: []string{"test-1", "test_2"},
			wantSQL: fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' "+
				"AND TABLE_SCHEMA IN ('db-1','db_2') AND TABLE_SCHEMA NOT IN ('test-1','test_2') "+
				"AND (TABLE_SCHEMA > '' OR (TABLE_SCHEMA = '' AND TABLE_NAME > '')) ORDER BY TABLE_SCHEMA ASC, TABLE_NAME ASC LIMIT %d", SelectBatchSize),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, err := buildTableInfosQueryInBatchSQL(tt.databaseInclude, tt.databaseExclude, tt.startSchema, tt.startTable, SelectBatchSize)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Equal(t, tt.errMsg, err.Error())
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantSQL, gotSQL)
			}
		})
	}
}
