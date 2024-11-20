package branch

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGetAllCreateTableStatements(t *testing.T) {
	//todo fixme
	//m, err := GetBranchSchema(MysqlHost, MysqlPort, MysqlUser, MysqlPass, []string{"mysql", "performance_schema", "information_schema", "sys"})
	//if err != nil {
	//	t.Error(err)
	//} else {
	//	for d, _ := range m {
	//		for t, _ := range m[d] {
	//			fmt.Printf("%v.%v: %v\n", d, t, m[d][t])
	//		}
	//	}
	//}
}

func TestGetSQLCreateDatabasesAndTables(t *testing.T) {
	tests := []struct {
		name             string
		createTableStmts map[string]map[string]string
		want             string
	}{
		{
			name: "Single database single table",
			createTableStmts: map[string]map[string]string{
				"db1": {
					"users": "CREATE TABLE users (id INT PRIMARY KEY)",
				},
			},
			want: "CREATE DATABASE IF NOT EXISTS db1;USE DATABASE db1;CREATE TABLE users (id INT PRIMARY KEY);",
		},
		{
			name: "Single database multiple tables",
			createTableStmts: map[string]map[string]string{
				"db1": {
					"users":  "CREATE TABLE users (id INT PRIMARY KEY)",
					"orders": "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)",
				},
			},
			want: "CREATE DATABASE IF NOT EXISTS db1;USE DATABASE db1;CREATE TABLE users (id INT PRIMARY KEY);CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);",
		},
		{
			name: "Multiple databases multiple tables",
			createTableStmts: map[string]map[string]string{
				"db1": {
					"users":  "CREATE TABLE users (id INT PRIMARY KEY)",
					"orders": "CREATE TABLE orders (id INT PRIMARY KEY)",
				},
				"db2": {
					"products": "CREATE TABLE products (id INT PRIMARY KEY)",
				},
			},
			want: "CREATE DATABASE IF NOT EXISTS db1;USE DATABASE db1;CREATE TABLE users (id INT PRIMARY KEY);CREATE TABLE orders (id INT PRIMARY KEY);CREATE DATABASE IF NOT EXISTS db2;USE DATABASE db2;CREATE TABLE products (id INT PRIMARY KEY);",
		},
		{
			name:             "Empty input",
			createTableStmts: map[string]map[string]string{},
			want:             "",
		},
		{
			name: "Database with no tables",
			createTableStmts: map[string]map[string]string{
				"db1": {},
			},
			want: "CREATE DATABASE IF NOT EXISTS db1;USE DATABASE db1;",
		},
		{
			name: "Complex table definitions",
			createTableStmts: map[string]map[string]string{
				"test_db": {
					"employees": `CREATE TABLE employees (
                        id INT PRIMARY KEY,
                        name VARCHAR(255),
                        department_id INT,
                        FOREIGN KEY (department_id) REFERENCES departments(id)
                    )`,
					"departments": `CREATE TABLE departments (
                        id INT PRIMARY KEY,
                        name VARCHAR(255)
                    )`,
				},
			},
			want: "CREATE DATABASE IF NOT EXISTS test_db;USE DATABASE test_db;CREATE TABLE employees (\n                        id INT PRIMARY KEY,\n                        name VARCHAR(255),\n                        department_id INT,\n                        FOREIGN KEY (department_id) REFERENCES departments(id)\n                    );CREATE TABLE departments (\n                        id INT PRIMARY KEY,\n                        name VARCHAR(255)\n                    );",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getSQLCreateDatabasesAndTables(tt.createTableStmts)
			if got != tt.want {
				t.Errorf("getSQLCreateDatabasesAndTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildTableInfoQuery(t *testing.T) {
	tests := []struct {
		name             string
		databasesExclude []string
		wantContains     string
	}{
		{
			name:             "No Exclude",
			databasesExclude: nil,
			wantContains:     "WHERE TABLE_TYPE = 'BASE TABLE'",
		},
		{
			name:             "With Exclude",
			databasesExclude: []string{"db1", "db2"},
			wantContains:     "AND TABLE_SCHEMA NOT IN ('db1','db2')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := buildTableInfosQuerySQL(tt.databasesExclude)
			if !strings.Contains(query, tt.wantContains) {
				t.Errorf("Query does not contain expected string. Got: %s, Want Contains: %s", query, tt.wantContains)
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
		databasesExclude []string
	}{
		{
			name:             "No Exclude",
			databasesExclude: nil,
		},
		{
			name:             "With Exclude",
			databasesExclude: []string{"eCommerce"},
		},
	}

	service := &SourceMySQLService{
		mysqlService: mysql,
	}

	for _, tt := range testcases {
		got, err := service.getTableInfos(tt.databasesExclude)
		assert.Nil(t, err)
		expected := make([]TableInfo, 0)
		for db, tables := range BranchSchemaForTest.schema {
			skip := false
			for _, dbToSkip := range tt.databasesExclude {
				if db == dbToSkip {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
			for table, _ := range tables {
				expected = append(expected, TableInfo{database: db, name: table})
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
