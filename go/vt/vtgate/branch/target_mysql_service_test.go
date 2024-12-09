package branch

import (
	"fmt"
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
	if first.TargetDBPattern != second.TargetDBPattern {
		fmt.Printf("TargetDBPattern different: first=%s, second=%s\n", first.TargetDBPattern, second.TargetDBPattern)
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

func TestGenerateTargetName(t *testing.T) {
	tests := []struct {
		sourceDBName          string
		targetDatabasePattern string
		expected              string
	}{
		{"example_db", fmt.Sprintf("target_%s_db", SourceDBNamePlaceHolder), "target_example_db_db"},
		{"dev_db", fmt.Sprintf("dev_%s_database", SourceDBNamePlaceHolder), "dev_dev_db_database"},
		{"prod", fmt.Sprintf("%s_production", SourceDBNamePlaceHolder), "prod_production"},
		{"test", fmt.Sprintf("%s", SourceDBNamePlaceHolder), "test"},
	}

	for _, test := range tests {
		result := GenerateTargetName(test.sourceDBName, test.targetDatabasePattern)
		if result != test.expected {
			t.Errorf("GenerateTargetName(%q, %q) = %q; expected %q", test.sourceDBName, test.targetDatabasePattern, result, test.expected)
		}
	}
}

func TestGenerateSourceName(t *testing.T) {
	tests := []struct {
		targetDBName          string
		targetDatabasePattern string
		expected              string
		expectError           bool
	}{
		{"target_example_db_db", fmt.Sprintf("target_%s_db", SourceDBNamePlaceHolder), "example_db", false},
		{"dev_dev_db_database", fmt.Sprintf("dev_%s_database", SourceDBNamePlaceHolder), "dev_db", false},
		{"prod_production", fmt.Sprintf("%s_production", SourceDBNamePlaceHolder), "prod", false},
		{"invalid_name", fmt.Sprintf("target_%s_db", SourceDBNamePlaceHolder), "", true},   // Should return an error
		{"target_wrong_db", fmt.Sprintf("other_%s_db", SourceDBNamePlaceHolder), "", true}, // Should return an error
		{"target_source_db", fmt.Sprintf("target_%s_db", SourceDBNamePlaceHolder), "source", false},
		{"target_db", fmt.Sprintf("target_%s_db", SourceDBNamePlaceHolder), "", true},
	}

	for _, test := range tests {
		result, err := GenerateSourceName(test.targetDBName, test.targetDatabasePattern)
		if test.expectError {
			if err == nil {
				t.Errorf("Expected error for GenerateSourceName(%q, %q), but got none", test.targetDBName, test.targetDatabasePattern)
			}
		} else {
			if err != nil {
				t.Errorf("Error for GenerateSourceName(%q, %q): %v", test.targetDBName, test.targetDatabasePattern, err)
			} else if result != test.expected {
				t.Errorf("GenerateSourceName(%q, %q) = %q; expected %q", test.targetDBName, test.targetDatabasePattern, result, test.expected)
			}
		}
	}
}
