/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"errors"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestParseDML(t *testing.T) {
	// DELETE
	tableName, whereExpr, _, err := parseDML("delete from t1 where id=1")
	whereExprStr := sqlparser.String(whereExpr)
	expectedTableName := "t1"
	expectedWhereExpr := "id = 1"
	assert.Equalf(t, expectedTableName, tableName, "table name")
	assert.Equalf(t, expectedWhereExpr, whereExprStr, "where expr")
	assert.Equal(t, nil, err)

	// UPDATE
	tableName, whereExpr, _, err = parseDML("update t2 set c1 = '123' where id=2")
	whereExprStr = sqlparser.String(whereExpr)
	expectedTableName = "t2"
	expectedWhereExpr = "id = 2"
	assert.Equalf(t, expectedTableName, tableName, "table name")
	assert.Equalf(t, expectedWhereExpr, whereExprStr, "where expr")
	assert.Equal(t, nil, err)

	// error: the type of sql is not supported
	_, _, _, err = parseDML("select * from t1")
	assert.Equalf(t, "the type of sql is not supported", err.Error(), "error message: %s", err.Error())

	// error: don't support join table now
	_, _, _, err = parseDML("update t1 join t2 on t1.id = t2.id set t1.c1 = '123' where t1.id = 1")
	assert.Equalf(t, "don't support join table now", err.Error(), "error message: %s", err.Error())

	// support alias
	_, _, _, err = parseDML("update t1 as mytable set mytable.c1 = '123' where mytable.id = 1")
	assert.Equal(t, err, nil)

	// error: the SQL should have where clause
	_, _, _, err = parseDML("delete from t1")
	assert.Equalf(t, "the SQL should have where clause", err.Error(), "error message: %s", err.Error())

	// error: the SQL should not have limit clause
	_, _, _, err = parseDML("delete from t1 where id=1 limit 1")
	assert.Equalf(t, "the SQL should not have limit clause", err.Error(), "error message: %s", err.Error())

	// error: the SQL should not have order clause
	_, _, _, err = parseDML("delete from t1 where id=1 order by c1")
	assert.Equalf(t, "the SQL should not have order by clause", err.Error(), "error message: %s", err.Error())
}

func TestGetTimeZoneStr(t *testing.T) {
	tests := []struct {
		offset int
		want   string
	}{
		{0, "UTC"},
		{60, "UTC+00:01:00"},
		{-60, "UTC-00:01:00"},
		{180, "UTC+00:03:00"},
		{-180, "UTC-00:03:00"},
		{3600, "UTC+01:00:00"},
	}

	for _, tt := range tests {
		if got := getTimeZoneStr(tt.offset); got != tt.want {
			t.Errorf("getTimeZoneStr(%d) = %s, want %s", tt.offset, got, tt.want)
		}
	}
}

func TestGetTimeZoneOffset(t *testing.T) {
	tests := []struct {
		timeZoneStr string
		wantOffset  int
		wantError   error
	}{
		{"UTC", 0, nil},
		{"UTC +01:02", 3720, errors.New("timeZoneStr is in wrong format")},

		{"UTC+01:02:03", 3723, nil},
		{"UTC-01:02:03", -3723, nil},
		{"UTC+12:11:22", 43882, nil},
		{"UTC+23:59:59", 86399, nil},
		{"UTC-23:59:59", -86399, nil},
	}

	for _, test := range tests {
		gotOffset, gotError := getTimeZoneOffset(test.timeZoneStr)
		if gotError == nil && test.wantError != nil {
			t.Errorf("getTimeZoneOffset(%s) didn't return error but expected it", test.timeZoneStr)
		}
		if gotError == nil && test.wantError == nil && gotOffset != test.wantOffset {
			t.Errorf("getTimeZoneOffset(%s) return %d but want %d", test.timeZoneStr, gotOffset, test.wantOffset)
		}
		if gotError != nil && test.wantError == nil {
			assert.Equalf(t, test.wantError.Error(), gotError.Error(), "error message: %s", gotError)
		}
		if gotError != nil && test.wantError != nil && gotError.Error() != test.wantError.Error() {
			t.Errorf("getTimeZoneOffset(%s) return error %s but want %s", test.timeZoneStr, gotError, test.wantError)
		}

	}
}
