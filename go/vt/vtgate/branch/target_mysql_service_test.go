package branch

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAllDatabases(t *testing.T) {
	mysqlService, mock := NewMockMysqlService(t)
	defer mysqlService.Close()
	TargetMySQLServiceForTest := &TargetMySQLService{
		mysqlService: mysqlService,
	}

	InitMockShowDatabases(mock)

	_, err := TargetMySQLServiceForTest.getAllDatabases()
	if err != nil {
		t.Error(err)
	}
}

func TestSelectBranchMeta(t *testing.T) {
	mysqlService, mock := NewMockMysqlService(t)
	defer mysqlService.Close()
	TargetMySQLServiceForTest := &TargetMySQLService{
		mysqlService: mysqlService,
	}

	InitMockBranchMetas(mock)

	for i, _ := range BranchMetasForTest {
		meta, err := TargetMySQLServiceForTest.selectBranchMeta(fmt.Sprintf("test%d", i))
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, true, compareBranchMetas(BranchMetasForTest[i], meta))
	}
	meta, err := TargetMySQLServiceForTest.selectBranchMeta("no this test")
	assert.Nil(t, meta)
	assert.NotNil(t, err)
}

func compareBranchMetas(first, second *BranchMeta) bool {
	if first == nil || second == nil {
		if first != second {
			fmt.Printf("One of the BranchMetas is nil: first=%v, second=%v\n", first, second)
		}
		return first == second
	}

	if first.name != second.name {
		fmt.Printf("name different: first=%s, second=%s\n", first.name, second.name)
		return false
	}
	if first.sourceHost != second.sourceHost {
		fmt.Printf("sourceHost different: first=%s, second=%s\n", first.sourceHost, second.sourceHost)
		return false
	}
	if first.sourcePort != second.sourcePort {
		fmt.Printf("sourcePort different: first=%d, second=%d\n", first.sourcePort, second.sourcePort)
		return false
	}
	if first.sourceUser != second.sourceUser {
		fmt.Printf("sourceUser different: first=%s, second=%s\n", first.sourceUser, second.sourceUser)
		return false
	}
	if first.sourcePassword != second.sourcePassword {
		fmt.Printf("sourcePassword different: first=%s, second=%s\n", first.sourcePassword, second.sourcePassword)
		return false
	}
	if first.targetDBPattern != second.targetDBPattern {
		fmt.Printf("targetDBPattern different: first=%s, second=%s\n", first.targetDBPattern, second.targetDBPattern)
		return false
	}
	if first.status != second.status {
		fmt.Printf("status different: first=%s, second=%s\n", first.status, second.status)
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

	if !sliceEqual(first.includeDatabases, second.includeDatabases) {
		fmt.Printf("includeDatabases different:\n first=%v\n second=%v\n",
			first.includeDatabases, second.includeDatabases)
		return false
	}
	if !sliceEqual(first.excludeDatabases, second.excludeDatabases) {
		fmt.Printf("excludeDatabases different:\n first=%v\n second=%v\n",
			first.excludeDatabases, second.excludeDatabases)
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
				"complex_table": "CREATE TABLE `complex_table` (id INT PRIMARY KEY,name VARCHAR(255))",
			},
			expected: map[string]string{
				"complex_table": "CREATE TABLE IF NOT EXISTS `complex_table` (id INT PRIMARY KEY,name VARCHAR(255))",
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
