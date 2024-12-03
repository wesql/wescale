package schemadiff

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestColumnCharset(t *testing.T) {
	testCase := []struct {
		schema1   string
		schema2   string
		DiffHints *DiffHints
		expect    string
	}{
		{
			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,

			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL COLLATE utf8mb4_0900_ai_ci,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict},
			"",
		},
		// todo enhancement?: it's a complex case, in this case, name should keep utf8mb4_0900_ai_ci. but we deal with table and column charset collate separately now,
		//  so it's hard to be resolved.
		{
			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,

			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL COLLATE utf8mb4_0900_ai_ci,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict},
			"ALTER TABLE `b2` COLLATE utf8mb4_general_ci",
		},
		{
			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,

			`CREATE TABLE b2 (
				 id int NOT NULL AUTO_INCREMENT,
				 name varchar(255) NOT NULL,
				 PRIMARY KEY (id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict},
			"ALTER TABLE `b2` MODIFY COLUMN `name` varchar(255) NOT NULL, COLLATE utf8mb4_general_ci",
		},
		{
			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci `,

			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateIgnoreAlways},
			"ALTER TABLE `b2` COLLATE utf8mb4_general_ci",
		},
		{
			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL COLLATE utf8mb4_general_ci,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB `,

			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL COLLATE utf8mb4_0900_ai_ci,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateIgnoreAlways},
			"",
		},
		{
			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL COLLATE utf8mb4_general_ci,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB `,

			`CREATE TABLE b2 (
		 id int NOT NULL AUTO_INCREMENT,
		 name varchar(255) NOT NULL COLLATE utf8mb4_0900_ai_ci,
		 PRIMARY KEY (id)
		) ENGINE=InnoDB `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict},
			"ALTER TABLE `b2` MODIFY COLUMN `name` varchar(255) COLLATE utf8mb4_0900_ai_ci NOT NULL",
		},
		{
			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  age1 int NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB `,

			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  age2 int NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict},
			"ALTER TABLE `b2` DROP COLUMN `age1`, ADD COLUMN `age2` int NOT NULL",
		},
		{
			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  age1 int NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB `,

			`CREATE TABLE b2 (
  id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  age2 int NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB `,
			&DiffHints{TableCharsetCollateStrategy: TableCharsetCollateStrict, ColumnCharsetCollateStrategy: ColumnCharsetCollateStrict, ColumnRenameStrategy: ColumnRenameHeuristicStatement},
			"ALTER TABLE `b2` RENAME COLUMN `age1` TO `age2`",
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

		diff, err := cte1.Diff(cte2, tt.DiffHints)
		assert.NoError(t, err)
		diffStr := diff.CanonicalStatementString()
		if diff != nil {
			print(diffStr)
		}
		assert.Equal(t, tt.expect, diffStr)

	}
}
