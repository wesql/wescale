package framework

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func ExecuteSqlScript(db *sql.DB, sqlScript string) error {
	_, err := db.Exec(sqlScript)
	return err
}

func ExecNoError(t *testing.T, db *sql.DB, sql string, args ...any) {
	t.Helper()
	log.Println(sql)
	_, err := db.Exec(sql, args...)
	assert.NoError(t, err)
}

func QueryNoError(t *testing.T, db *sql.DB, sql string, args ...any) *sql.Rows {
	t.Helper()
	log.Println(sql)
	rows, err := db.Query(sql, args...)
	assert.NoError(t, err)
	return rows
}

func ExecWithErrorContains(t *testing.T, db *sql.DB, contains string, sql string, args ...any) {
	t.Helper()
	log.Println(sql)
	_, err := db.Exec(sql, args...)
	assert.ErrorContains(t, err, contains)
}

func QueryWithErrorContains(t *testing.T, db *sql.DB, contains string, sql string, args ...any) {
	t.Helper()
	log.Println(sql)
	_, err := db.Query(sql, args...)
	assert.ErrorContains(t, err, contains)
}

// CheckTableExists checks if a specific table exists in a given schema.
func CheckTableExists(t *testing.T, db *sql.DB, schema string, table string) bool {
	query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", schema, table)
	rows := QueryNoError(t, db, query)
	defer rows.Close()
	for rows.Next() {
		var count int64
		err := rows.Scan(&count)
		assert.NoError(t, err)
		return count > 0
	}

	return false
}

func CheckColumnExists(t *testing.T, db *sql.DB, schema, table, column string) bool {
	t.Helper()
	query := fmt.Sprintf(`SELECT COUNT(1) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'`, schema, table, column)

	rows := QueryNoError(t, db, query)
	defer rows.Close()

	var count int
	if rows.Next() {
		err := rows.Scan(&count)
		assert.NoError(t, err)
	}

	return count > 0
}
