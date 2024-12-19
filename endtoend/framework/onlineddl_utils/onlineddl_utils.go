package onlineddl_utils

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func VtgateExecDDL(t *testing.T, db *sql.DB, ddlStrategy string, ddl string, expectError string) string {
	t.Helper()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	// Read original DDL strategy
	var originalStrategy string
	err = conn.QueryRowContext(ctx, "SELECT @@ddl_strategy").Scan(&originalStrategy)
	assert.NoError(t, err)

	// Set new DDL strategy
	_, err = conn.ExecContext(ctx, fmt.Sprintf("SET @@ddl_strategy='%s'", ddlStrategy))
	assert.NoError(t, err)

	// Ensure strategy is reset after execution
	defer func() {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("SET @@ddl_strategy='%s'", originalStrategy))
		assert.NoError(t, err)
	}()

	// Execute DDL
	var uuid string
	err = conn.QueryRowContext(ctx, ddl).Scan(&uuid)

	// Handle expected errors
	if expectError == "" {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectError)
	}

	return uuid
}
