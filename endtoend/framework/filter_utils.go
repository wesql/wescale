package framework

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func AssertFilterExists(t *testing.T, filterName string, db *sql.DB) {
	t.Helper()
	row := QueryNoError(t, db, fmt.Sprintf("show create filter %s", filterName))
	defer row.Close()
	assert.True(t, row.Next())
}

func CreateFilterIfNotExists(t *testing.T, builder *FilterBuilder, db *sql.DB) {
	t.Helper()
	sql, err := builder.Build()
	assert.NoError(t, err)

	ExecNoError(t, db, sql)
}

func DropFilter(t *testing.T, filterName string, db *sql.DB) {
	t.Helper()
	ExecNoError(t, db, fmt.Sprintf("drop filter %s", filterName))
}
