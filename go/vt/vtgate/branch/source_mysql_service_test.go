package branch

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetBranchSchemaInBatches(t *testing.T) {
	service, mock := NewMockMysqlService(t)
	s := &SourceMySQLService{mysqlService: service}

	InitMockShowCreateTable(mock)
	tableInfo := make([]TableInfo, 0)
	for d, tables := range BranchSchemaForTest.schema {
		for table, _ := range tables {
			tableInfo = append(tableInfo, TableInfo{
				database: d,
				name:     table,
			})
		}
	}

	// get one table schema each time, because it's more convenient for mock
	got, err := s.getBranchSchemaInBatches(tableInfo, 1)
	assert.Nil(t, err)
	compareBranchSchema(t, BranchSchemaForTest, got)
}

func TestGetBranchSchema(t *testing.T) {
	service, mock := NewMockMysqlService(t)
	s := &SourceMySQLService{mysqlService: service}

	// get one table schema each time, because it's more convenient for mock
	GetBranchSchemaBatchSize = 1

	InitMockTableInfos(mock)
	InitMockShowCreateTable(mock)

	got, err := s.GetBranchSchema([]string{"*"}, nil)
	if err != nil {
		t.Error(err)
	}

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
	assert.Equal(t, len(want.schema), len(got.schema), "schema databases length mismatch")

	// Compare each database's tables
	for dbName, wantTables := range want.schema {
		gotTables, exists := got.schema[dbName]
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
		wantSQL         string
		wantErr         bool
		errMsg          string
	}{
		{
			name:            "Normal case with include and exclude",
			databaseInclude: []string{"db1", "db2"},
			databaseExclude: []string{"test1", "test2"},
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('db1','db2') AND TABLE_SCHEMA NOT IN ('test1','test2')",
			wantErr:         false,
		},
		{
			name:            "Normal case with include and exclude",
			databaseInclude: []string{"db1", "db2", "", ""},
			databaseExclude: []string{"test1", "test2", "", ""},
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('db1','db2') AND TABLE_SCHEMA NOT IN ('test1','test2')",
			wantErr:         false,
		},
		{
			name:            "Only include databases",
			databaseInclude: []string{"db1", "db2"},
			databaseExclude: nil,
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('db1','db2')",
			wantErr:         false,
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
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('test1')",
			wantErr:         false,
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
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
			wantErr:         false,
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
			wantSQL:         "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ('db-1','db_2') AND TABLE_SCHEMA NOT IN ('test-1','test_2')",
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, err := buildTableInfosQuerySQL(tt.databaseInclude, tt.databaseExclude)

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

func TestGetTableInfos(t *testing.T) {
	mysql, mock := NewMockMysqlService(t)
	defer mysql.Close()

	InitMockTableInfos(mock)

	testcases := []struct {
		name             string
		databasesInclude []string
		databasesExclude []string
		wantErr          bool
	}{
		{
			name:             "With Include",
			databasesInclude: []string{"eCommerce"},
		},
		{
			name:             "With Exclude",
			databasesInclude: []string{"*"},
			databasesExclude: []string{"eCommerce"},
		},
		{
			name:             "Include nothing",
			databasesInclude: nil,
			wantErr:          true,
		},
		{
			name:             "Include nothing",
			databasesInclude: []string{""},
			wantErr:          true,
		},
		{
			name:             "Exclude every thing",
			databasesExclude: []string{"*"},
			wantErr:          true,
		},
		{
			name:             "Exclude nothing",
			databasesInclude: []string{"*"},
		},
	}

	service := &SourceMySQLService{
		mysqlService: mysql,
	}

	for _, tt := range testcases {
		got, err := service.getTableInfos(tt.databasesInclude, tt.databasesExclude)
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}
		assert.Nil(t, err)
		expected := make([]TableInfo, 0)
		for db, tables := range BranchSchemaForTest.schema {
			exclude := false
			for _, dbToExclude := range tt.databasesExclude {
				if db == dbToExclude {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}

			include := false
			for _, dbToInclude := range tt.databasesInclude {
				if db == dbToInclude || dbToInclude == "*" {
					include = true
					break
				}
			}
			if include {
				for table, _ := range tables {
					expected = append(expected, TableInfo{database: db, name: table})
				}
			}
		}

		assert.Equal(t, len(expected), len(got))

		m := make(map[string]int)
		for _, table := range got {
			m[table.database+table.name]++
		}
		for _, table := range expected {
			if count, exist := m[table.database+table.name]; !exist || count <= 0 {
				t.Errorf("Table %s not found in got", table)
			}
			m[table.database+table.name]--
		}
	}
}
