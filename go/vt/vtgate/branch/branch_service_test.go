package branch

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"strings"
	"testing"
	"vitess.io/vitess/go/vt/schemadiff"
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

func TestGetBranchDiff(t *testing.T) {
	tests := []struct {
		name         string
		originSchema *BranchSchema
		expectSchema *BranchSchema
		wantDiff     *BranchDiff
		wantErr      bool
	}{
		{
			name: "add new database",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
					"db2": {
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table1": {},
						},
					},
					"db2": {
						needCreate: true,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table2": {
								"CREATE TABLE `db2`.`table2` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "drop database",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
					"db2": {
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table1": {},
						},
					},
					"db2": {
						needCreate: false,
						needDrop:   true,
						tableDDLs:  map[string][]string{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "modify table structure in existing database",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY, name VARCHAR(255))",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table1": {
								"ALTER TABLE `db1`.`table1` ADD COLUMN `name` varchar(255)",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "add new table in existing database",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table1": {},
							"table2": {
								"CREATE TABLE `db1`.`table2` (\n\t`id` int,\n\tPRIMARY KEY (`id`)\n)",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "drop table in existing database",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"table1": {},
							"table2": {
								"DROP TABLE `table2`",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "complex case - multiple databases with various changes",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"users":    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
						"orders":   "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)",
						"old_logs": "CREATE TABLE old_logs (id INT PRIMARY KEY, log_data TEXT)",
					},
					"db2": {
						"products": "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(200))",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"users":    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(255))",
						"orders":   "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, order_date TIMESTAMP)",
						"payments": "CREATE TABLE payments (id INT PRIMARY KEY, order_id INT, amount DECIMAL(10,2))",
					},
					"db3": {
						"analytics": "CREATE TABLE analytics (id INT PRIMARY KEY, event_type VARCHAR(50), data JSON)",
					},
				},
			},
			wantDiff: &BranchDiff{
				diffs: map[string]*DatabaseDiff{
					"db1": {
						needCreate: false,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"old_logs": {
								"DROP TABLE `old_logs`",
							},
							"orders": {
								"ALTER TABLE `db1`.`orders` ADD COLUMN `order_date` timestamp NULL",
							},
							"payments": {
								"CREATE TABLE `db1`.`payments` (\n\t`id` int,\n\t`order_id` int,\n\t`amount` decimal(10,2),\n\tPRIMARY KEY (`id`)\n)",
							},
							"users": {
								"ALTER TABLE `db1`.`users` ADD COLUMN `email` varchar(255)",
							},
						},
					},
					"db2": {
						needCreate: false,
						needDrop:   true,
						tableDDLs:  map[string][]string{},
					},
					"db3": {
						needCreate: true,
						needDrop:   false,
						tableDDLs: map[string][]string{
							"analytics": {
								"CREATE TABLE `db3`.`analytics` (\n\t`id` int,\n\t`event_type` varchar(50),\n\t`data` json,\n\tPRIMARY KEY (`id`)\n)",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "error case - invalid schema",
			originSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "INVALID SQL STATEMENT",
					},
				},
			},
			expectSchema: &BranchSchema{
				schema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			wantDiff: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hints := &schemadiff.DiffHints{}
			gotDiff, err := getBranchDiff(tt.originSchema, tt.expectSchema, hints)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			compareBranchDiff(t, tt.wantDiff, gotDiff)
		})
	}
}

// compareDatabaseDiff compares two DatabaseDiff objects in detail
func compareDatabaseDiff(t *testing.T, want, got *DatabaseDiff) {
	if want == nil || got == nil {
		assert.Equal(t, want, got, "One of the DatabaseDiff is nil")
		return
	}

	assert.Equal(t, want.needCreate, got.needCreate, "needCreate mismatch")
	assert.Equal(t, want.needDrop, got.needDrop, "needDrop mismatch")

	// Compare tableDDLs maps
	assert.Equal(t, len(want.tableDDLs), len(got.tableDDLs), "tableDDLs length mismatch")
	for tableName, wantDDLs := range want.tableDDLs {
		gotDDLs, exists := got.tableDDLs[tableName]
		assert.True(t, exists, "missing table DDLs for table %s", tableName)
		if exists {
			// Sort DDLs to ensure consistent comparison
			sort.Strings(wantDDLs)
			sort.Strings(gotDDLs)
			assert.Equal(t, wantDDLs, gotDDLs, "DDLs mismatch for table %s", tableName)
		}
	}
}

// compareBranchDiff compares two BranchDiff objects in detail
func compareBranchDiff(t *testing.T, want, got *BranchDiff) {
	if want == nil || got == nil {
		assert.Equal(t, want, got, "One of the BranchDiff is nil")
		return
	}

	// Compare diffs maps
	assert.Equal(t, len(want.diffs), len(got.diffs), "diffs length mismatch")

	// Get sorted database names for consistent comparison
	var wantDBNames, gotDBNames []string
	for dbName := range want.diffs {
		wantDBNames = append(wantDBNames, dbName)
	}
	for dbName := range got.diffs {
		gotDBNames = append(gotDBNames, dbName)
	}
	sort.Strings(wantDBNames)
	sort.Strings(gotDBNames)

	assert.Equal(t, wantDBNames, gotDBNames, "database names mismatch")

	// Compare each database diff
	for _, dbName := range wantDBNames {
		wantDB := want.diffs[dbName]
		gotDB := got.diffs[dbName]
		assert.NotNil(t, gotDB, "missing DatabaseDiff for database %s", dbName)
		if gotDB != nil {
			compareDatabaseDiff(t, wantDB, gotDB)
		}
	}
}
