/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package wrangler

import (
	"fmt"
	"testing"

	"github.com/wesql/wescale/go/vt/schemadiff"
)

func TestRemoveComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "No Comments",
			input:    "select * from table1",
			expected: "select * from table1",
		},
		{
			name:     "Single Line Comment",
			input:    "select * from table1 -- This is a comment",
			expected: "select * from table1",
		},
		{
			name:     "Multi Line Comment",
			input:    "select * from table1 /* Multi line\nComment */",
			expected: "select * from table1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := removeComments(tt.input)
			if err != nil {
				t.Errorf("removeComments(%s) got unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("removeComments(%s) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSchemasConflict(t *testing.T) {

	snapshotSchemaStr := `CREATE TABLE foo (
						id INT NOT NULL,
						col1 INT NOT NULL,
						col2 VARCHAR(255) NOT NULL,
						PRIMARY KEY(id),
						KEY col1_index(col1)
				       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	snapshotSchema, err := schemadiff.NewSchemaFromQueries([]string{snapshotSchemaStr})
	if err != nil {
		t.Errorf("NewSchemaFromQueries(%s) got unexpected error: %v", snapshotSchemaStr, err)
	}

	addIntCol3BasedOnSnapshot := `CREATE TABLE foo (
								id INT NOT NULL,
								col1 INT NOT NULL,
								col2 VARCHAR(255) NOT NULL,
								col3 INT NOT NULL,
								PRIMARY KEY(id),
                 				KEY col1_index(col1)
						   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	addVarcharCol3BasedOnSnapshot := `CREATE TABLE foo (
								id INT NOT NULL,
								col1 INT NOT NULL,
								col2 VARCHAR(255) NOT NULL,
								col3 VARCHAR(255) NOT NULL,
								PRIMARY KEY(id),
                 				KEY col1_index(col1)
						   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	dropCol1BasedOnSnapshot := `CREATE TABLE foo (
								id INT NOT NULL,
								col2 VARCHAR(255) NOT NULL,
								PRIMARY KEY(id)
						   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	dropCol2BasedOnSnapshot := `CREATE TABLE foo (
								id INT NOT NULL,
								col1 INT NOT NULL,
								PRIMARY KEY(id),
								KEY col1_index(col1)
						   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	addIndexCol2BasedOnSnapshot := `CREATE TABLE foo (
						id INT NOT NULL,
						col1 INT NOT NULL,
						col2 VARCHAR(255) NOT NULL,
						PRIMARY KEY(id),
						KEY col1_index(col1),
						KEY col2_index(col2)
				       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	dropIndexCol1BasedOnSnapshot := `CREATE TABLE foo (
						id INT NOT NULL,
						col1 INT NOT NULL,
						col2 VARCHAR(255) NOT NULL,
						PRIMARY KEY(id)
				       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	tests := []struct {
		name             string
		schema1Str       string
		schema2Str       string
		expectedConflict bool
	}{
		{
			name:             "1.schema1 add int column3, schema2 no changes, no conflict",
			schema1Str:       addIntCol3BasedOnSnapshot,
			schema2Str:       snapshotSchemaStr,
			expectedConflict: false,
		},
		{
			name:             "2.schema1 add int column3, schema2 add varchar col3, conflict (different col order)",
			schema1Str:       addIntCol3BasedOnSnapshot,
			schema2Str:       addVarcharCol3BasedOnSnapshot,
			expectedConflict: true,
		},
		{
			name:             "3.schema1 add int column3, schema2 drop col1, no conflict (both remain id, col2, col3)",
			schema1Str:       addIntCol3BasedOnSnapshot,
			schema2Str:       dropCol1BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "4.schema1 add int column3, schema2 add index col2, no conflict",
			schema1Str:       addIntCol3BasedOnSnapshot,
			schema2Str:       addIndexCol2BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "5.schema1 add int column3, schema2 drop index col1, no conflict",
			schema1Str:       addIntCol3BasedOnSnapshot,
			schema2Str:       dropIndexCol1BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "6.schema1 drop col1, schema2 drop col2, no conflict (both remain id)",
			schema1Str:       dropCol1BasedOnSnapshot,
			schema2Str:       dropCol2BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "7.schema1 drop col1, schema2 add index col2, no conflict",
			schema1Str:       dropCol1BasedOnSnapshot,
			schema2Str:       addIndexCol2BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "8.schema1 drop col2, schema2 add index col2, conflict",
			schema1Str:       dropCol2BasedOnSnapshot,
			schema2Str:       addIndexCol2BasedOnSnapshot,
			expectedConflict: true,
		},
		{
			name:             "8.schema1 drop col2, schema2 drop index col1, no conflict",
			schema1Str:       dropCol2BasedOnSnapshot,
			schema2Str:       dropIndexCol1BasedOnSnapshot,
			expectedConflict: false,
		},
		{
			name:             "9.schema1 add index col2, schema2 drop index col1, no conflict",
			schema1Str:       addIndexCol2BasedOnSnapshot,
			schema2Str:       dropIndexCol1BasedOnSnapshot,
			expectedConflict: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema1, err := schemadiff.NewSchemaFromQueries([]string{tt.schema1Str})
			if err != nil {
				t.Errorf("NewSchemaFromQueries(%s) got unexpected error: %v", tt.schema1Str, err)
			}
			schema2, err := schemadiff.NewSchemaFromQueries([]string{tt.schema2Str})
			if err != nil {
				t.Errorf("NewSchemaFromQueries(%s) got unexpected error: %v", tt.schema2Str, err)
			}

			conflict, message, err := SchemasConflict(schema1, schema2, snapshotSchema)
			if err != nil {
				t.Errorf("SchemasConflict got unexpected error: %v", err)
			}

			if conflict != tt.expectedConflict {
				t.Errorf("SchemasConflict(%s,%s) = %v, want %v", tt.schema1Str, tt.schema2Str, conflict, tt.expectedConflict)
			}

			if tt.expectedConflict == true {
				fmt.Printf("conflict message:%s", message)
			}

		})
	}

}
