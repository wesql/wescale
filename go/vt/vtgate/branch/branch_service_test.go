package branch

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"vitess.io/vitess/go/vt/schemadiff"
)

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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
					"db2": {
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "CREATE TABLE table1 (id INT PRIMARY KEY)",
						"table2": "CREATE TABLE table2 (id INT PRIMARY KEY)",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
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
				branchSchema: map[string]map[string]string{
					"db1": {
						"table1": "INVALID SQL STATEMENT",
					},
				},
			},
			expectSchema: &BranchSchema{
				branchSchema: map[string]map[string]string{
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
			gotDiff, err := getBranchSchemaDiff(tt.originSchema, tt.expectSchema, hints)

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
