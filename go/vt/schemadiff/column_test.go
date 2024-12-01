package schemadiff

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestColumnCharset(t *testing.T) {
	testCase := []struct {
		schema1 string
		schema2 string
		expect  bool
	}{
		{
			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,
			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,
			true,
		},
	}

	for _, tt := range testCase {
		stmt1, err := sqlparser.Parse(tt.schema1)
		assert.NoError(t, err)
		ctstmt1 := stmt1.(*sqlparser.CreateTable)
		cte1 := &CreateTableEntity{CreateTable: ctstmt1}

		stmt2, err := sqlparser.Parse(tt.schema2)
		assert.NoError(t, err)
		ctstmt2 := stmt2.(*sqlparser.CreateTable)
		cte2 := &CreateTableEntity{CreateTable: ctstmt2}

		diff, err := cte1.Diff(cte2, &DiffHints{})
		assert.NoError(t, err)
		if diff != nil {
			print(diff.CanonicalStatementString())
		}

	}
}
