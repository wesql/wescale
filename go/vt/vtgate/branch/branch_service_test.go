package branch

import (
	"reflect"
	"strings"
	"testing"
)

func TestFilterBranchSchema(t *testing.T) {
	// Create test data
	testStmts := map[string]map[string]string{
		"db1": {
			"users":    "CREATE TABLE users ...",
			"orders":   "CREATE TABLE orders ...",
			"tmp_test": "CREATE TABLE tmp_test ...",
		},
		"db2": {
			"users":      "CREATE TABLE users ...",
			"products":   "CREATE TABLE products ...",
			"categories": "CREATE TABLE categories ...",
		},
		"test_db": {
			"test_table": "CREATE TABLE test_table ...",
			"temp_table": "CREATE TABLE temp_table ...",
		},
	}

	// Define test cases
	tests := []struct {
		name          string
		include       string
		exclude       string
		expectedErr   string
		expectedStmts map[string]map[string]string
	}{
		{
			name:        "Empty include pattern",
			include:     "",
			exclude:     "",
			expectedErr: "include pattern is empty",
		},
		{
			name:        "Non-existent pattern",
			include:     "db3.*",
			exclude:     "",
			expectedErr: "the following include patterns had no matches: db3.*",
		},
		{
			name:    "Match all users tables",
			include: "*.users",
			exclude: "",
			expectedStmts: map[string]map[string]string{
				"db1": {"users": "CREATE TABLE users ..."},
				"db2": {"users": "CREATE TABLE users ..."},
			},
		},
		{
			name:    "Match specific database with exclusion",
			include: "db1.*",
			exclude: "db1.tmp_*",
			expectedStmts: map[string]map[string]string{
				"db1": {
					"users":  "CREATE TABLE users ...",
					"orders": "CREATE TABLE orders ...",
				},
			},
		},
		{
			name:    "Multiple patterns",
			include: "db1.users, db2.products",
			exclude: "",
			expectedStmts: map[string]map[string]string{
				"db1": {"users": "CREATE TABLE users ..."},
				"db2": {"products": "CREATE TABLE products ..."},
			},
		},
		{
			name:        "Multiple patterns with one non-existent",
			include:     "db1.users, nonexistent.table",
			exclude:     "",
			expectedErr: "the following include patterns had no matches: nonexistent.table",
		},
		{
			name:    "Wildcard with specific exclusion",
			include: "*.*",
			exclude: "*.tmp_*, *.temp_*",
			expectedStmts: map[string]map[string]string{
				"db1": {
					"users":  "CREATE TABLE users ...",
					"orders": "CREATE TABLE orders ...",
				},
				"db2": {
					"users":      "CREATE TABLE users ...",
					"products":   "CREATE TABLE products ...",
					"categories": "CREATE TABLE categories ...",
				},
				"test_db": {
					"test_table": "CREATE TABLE test_table ...",
				},
			},
		},
	}

	// Run test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := &BranchSchema{schema: testStmts}
			err := filterBranchSchema(schema, tc.include, tc.exclude)

			// Check error
			if tc.expectedErr != "" {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tc.expectedErr)
				} else if !strings.Contains(err.Error(), tc.expectedErr) {
					t.Errorf("expected error containing %q, got %q", tc.expectedErr, err.Error())
				}
				return
			}

			// Check result when no error is expected
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(schema.schema, tc.expectedStmts) {
				t.Errorf("expected %v, got %v", tc.expectedStmts, schema.schema)
			}
		})
	}
}

func TestMatchPattern(t *testing.T) {
	// Define test cases
	tests := []struct {
		name    string
		tableId string
		pattern string
		want    bool
	}{
		// Basic matching tests
		{
			name:    "Exact match",
			tableId: "db1.users",
			pattern: "db1.users",
			want:    true,
		},
		{
			name:    "No match",
			tableId: "db1.users",
			pattern: "db2.orders",
			want:    false,
		},

		// Full wildcard tests
		{
			name:    "Full wildcard match",
			tableId: "db1.users",
			pattern: "*.*",
			want:    true,
		},
		{
			name:    "Database wildcard match",
			tableId: "db1.users",
			pattern: "*.users",
			want:    true,
		},
		{
			name:    "Table wildcard match",
			tableId: "db1.users",
			pattern: "db1.*",
			want:    true,
		},

		// Partial wildcard tests
		{
			name:    "Prefix wildcard match",
			tableId: "test_db.users",
			pattern: "test_*.users",
			want:    true,
		},
		{
			name:    "Prefix wildcard no match",
			tableId: "db_test.users",
			pattern: "test_*.users",
			want:    false,
		},
		{
			name:    "Suffix wildcard match",
			tableId: "db1.tmp_test",
			pattern: "db1.*_test",
			want:    true,
		},
		{
			name:    "Suffix wildcard no match",
			tableId: "db1.test_tmp",
			pattern: "db1.*_test",
			want:    false,
		},

		// Multiple wildcard tests
		{
			name:    "Multiple wildcards match",
			tableId: "test_db1_prod.temp_users_2024",
			pattern: "test_*_prod.*_users_*",
			want:    true,
		},
		{
			name:    "Multiple wildcards no match",
			tableId: "test_db1_dev.temp_users_2024",
			pattern: "test_*_prod.*_users_*",
			want:    false,
		},

		// Special character tests
		{
			name:    "Pattern with brackets match",
			tableId: "db1.[test]_table",
			pattern: "db1.[test]_*",
			want:    true,
		},
		{
			name:    "Pattern with dots match",
			tableId: "db1.user.table",
			pattern: "db1.user.table",
			want:    false, // Should be false because we expect only one dot as separator
		},

		// Edge cases
		{
			name:    "Empty table ID",
			tableId: "",
			pattern: "*.*",
			want:    false,
		},
		{
			name:    "Empty pattern",
			tableId: "db1.users",
			pattern: "",
			want:    false,
		},
		{
			name:    "Invalid table ID format",
			tableId: "db1_users",
			pattern: "*.*",
			want:    false,
		},
		{
			name:    "Invalid pattern format",
			tableId: "db1.users",
			pattern: "db1_users",
			want:    false,
		},
		{
			name:    "Pattern with spaces",
			tableId: "db1.users",
			pattern: " db1.users ",
			want:    true, // Should match because we trim spaces
		},

		// Common use cases for database operations
		{
			name:    "Temporary table match",
			tableId: "db1.tmp_users_2024",
			pattern: "*.tmp_*",
			want:    true,
		},
		{
			name:    "Backup table match",
			tableId: "db1.users_backup_2024",
			pattern: "*.users_backup_*",
			want:    true,
		},
		{
			name:    "Test database match",
			tableId: "test_db.users",
			pattern: "test_*.*",
			want:    true,
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchPattern(tt.tableId, tt.pattern)
			if got != tt.want {
				t.Errorf("matchPattern(%q, %q) = %v, want %v",
					tt.tableId, tt.pattern, got, tt.want)
			}
		})
	}
}
