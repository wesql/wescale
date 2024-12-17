package branch

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAllDatabases(t *testing.T) {
	mysqlService, mock := NewMockMysqlService(t)
	defer mysqlService.Close()
	TargetMySQLServiceForTest := NewTargetMySQLService(mysqlService)

	InitMockShowDatabases(mock)

	_, err := TargetMySQLServiceForTest.getAllDatabases()
	if err != nil {
		t.Error(err)
	}
}

func compareBranchMetas(first, second *BranchMeta) bool {
	if first == nil || second == nil {
		if first != second {
			fmt.Printf("One of the BranchMetas is nil: first=%v, second=%v\n", first, second)
		}
		return first == second
	}

	if first.Name != second.Name {
		fmt.Printf("Name different: first=%s, second=%s\n", first.Name, second.Name)
		return false
	}
	if first.SourceHost != second.SourceHost {
		fmt.Printf("SourceHost different: first=%s, second=%s\n", first.SourceHost, second.SourceHost)
		return false
	}
	if first.SourcePort != second.SourcePort {
		fmt.Printf("SourcePort different: first=%d, second=%d\n", first.SourcePort, second.SourcePort)
		return false
	}
	if first.SourceUser != second.SourceUser {
		fmt.Printf("SourceUser different: first=%s, second=%s\n", first.SourceUser, second.SourceUser)
		return false
	}
	if first.SourcePassword != second.SourcePassword {
		fmt.Printf("SourcePassword different: first=%s, second=%s\n", first.SourcePassword, second.SourcePassword)
		return false
	}
	if first.Status != second.Status {
		fmt.Printf("Status different: first=%s, second=%s\n", first.Status, second.Status)
		return false
	}

	sliceEqual := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	if !sliceEqual(first.IncludeDatabases, second.IncludeDatabases) {
		fmt.Printf("IncludeDatabases different:\n first=%v\n second=%v\n",
			first.IncludeDatabases, second.IncludeDatabases)
		return false
	}
	if !sliceEqual(first.ExcludeDatabases, second.ExcludeDatabases) {
		fmt.Printf("ExcludeDatabases different:\n first=%v\n second=%v\n",
			first.ExcludeDatabases, second.ExcludeDatabases)
		return false
	}

	return true
}

func TestAddIfNotExistsForCreateTableSQL(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name: "Basic test",
			input: map[string]string{
				"users": "CREATE TABLE users (id INT PRIMARY KEY)",
			},
			expected: map[string]string{
				"users": "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)",
			},
		},
		{
			name: "Already has IF NOT EXISTS",
			input: map[string]string{
				"posts": "CREATE TABLE IF NOT EXISTS posts (id INT PRIMARY KEY)",
			},
			expected: map[string]string{
				"posts": "CREATE TABLE IF NOT EXISTS posts (id INT PRIMARY KEY)",
			},
		},
		{
			name: "With backticks",
			input: map[string]string{
				"comments": "CREATE TABLE `comments` (id INT PRIMARY KEY)",
			},
			expected: map[string]string{
				"comments": "CREATE TABLE IF NOT EXISTS `comments` (id INT PRIMARY KEY)",
			},
		},
		{
			name: "With single quotes",
			input: map[string]string{
				"tags": "CREATE TABLE tags (id INT PRIMARY KEY)",
			},
			expected: map[string]string{
				"tags": "CREATE TABLE IF NOT EXISTS tags (id INT PRIMARY KEY)",
			},
		},
		{
			name: "Complex table",
			input: map[string]string{
				"complex_table": "CREATE TABLE `complex_table` (id INT PRIMARY KEY,Name VARCHAR(255))",
			},
			expected: map[string]string{
				"complex_table": "CREATE TABLE IF NOT EXISTS `complex_table` (id INT PRIMARY KEY,Name VARCHAR(255))",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addIfNotExistsForCreateTableSQL(tt.input)

			for tableName, sql := range tt.expected {
				if resultSQL, ok := result[tableName]; !ok {
					t.Errorf("table %s not found in result", tableName)
				} else if resultSQL != sql {
					t.Errorf("for table %s\nexpected: %s\ngot: %s",
						tableName, sql, resultSQL)
				}
			}
		})
	}
}

func TestNormalizeCreateTableSQL(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		expectError bool
	}{
		{
			input:       "CREATE TABLE `users` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `username` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,\n  `email` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n  `col1` int DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `email` (`email`)\n) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			expected:    "create table if not exists `users` (\n\tid int not null auto_increment,\n\tusername varchar(50) character set utf8mb4 collate utf8mb4_general_ci not null,\n\temail varchar(100) character set utf8mb4 collate utf8mb4_general_ci default null,\n\tcreated_at timestamp null default current_timestamp(),\n\tcol1 int default null,\n\tPRIMARY KEY (id),\n\tUNIQUE KEY email (email)\n) CHARSET utf8mb4,\n  COLLATE utf8mb4_general_ci",
			expectError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			output, err := normalizeCreateTableSQL(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %s", err)
				}
				assert.Equal(t, tt.expected, output)
			}
		})
	}

}
