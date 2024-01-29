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
	type args struct {
		sql string
	}

	tests := []struct {
		name              string
		args              args
		expectedTableName string
		expectedWhereExpr string
		expectedErrMsg    string
	}{
		{
			name: "delete",
			args: args{
				sql: "delete from t1 where id=1",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "id = 1",
			expectedErrMsg:    "",
		},
		{
			name: "update",
			args: args{
				sql: "update t2 set c1 = '123' where id=2",
			},
			expectedTableName: "t2",
			expectedWhereExpr: "id = 2",
			expectedErrMsg:    "",
		},
		{
			name: "table alias",
			args: args{
				sql: "update t3 as mytable set mytable.c1 = '123' where mytable.id = 3",
			},
			expectedTableName: "t3 as mytable",
			expectedWhereExpr: "mytable.id = 3",
			expectedErrMsg:    "",
		},
		{
			name: "table schema",
			args: args{
				sql: "update mytableSchema.t3 as mytable set mytable.c1 = '123' where mytable.id = 3",
			},
			expectedTableName: "mytableSchema.t3 as mytable",
			expectedWhereExpr: "mytable.id = 3",
			expectedErrMsg:    "",
		},
		{
			name: "error: select is not supported",
			args: args{
				sql: "select * from t1",
			},
			expectedErrMsg: "the type of sql is not supported",
		},
		{
			name: "error: join is not supported",
			args: args{
				sql: "update t1 join t2 on t1.id = t2.id set t1.c1 = '123' where t1.id = 1",
			},
			expectedErrMsg: "don't support join table now",
		},
		{
			name: "error: should have where clause",
			args: args{
				sql: "delete from t1",
			},
			expectedErrMsg: "the SQL should have where clause",
		},
		{
			name: "error: should not have limit clause",
			args: args{
				sql: "delete from t1 where id=1 limit 1",
			},
			expectedErrMsg: "the SQL should not have limit clause",
		},
		{
			name: "error: should not have order clause",
			args: args{
				sql: "delete from t1 where id=1 order by c1",
			},
			expectedErrMsg: "the SQL should not have order by clause",
		},
		{
			name: "subquery in update where",
			args: args{
				sql: "update t1 set c1 = '123' where c2 in (select c2 from t2)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "c2 in (select c2 from t2)",
			expectedErrMsg:    "",
		},
		{
			name: "subquery in delete where",
			args: args{
				sql: "delete from t1 where c2 in (select c2 from t2)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "c2 in (select c2 from t2)",
			expectedErrMsg:    "",
		},
		{
			name: "subquery in update set",
			args: args{
				sql: "update t1 set c1 = (select c1 from t2 limit 1) where 1 = 1",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "1 = 1",
			expectedErrMsg:    "",
		},
		{
			name: "subquery in update where exist",
			args: args{
				sql: "update t1 set c1 = '123' where exists (select c2 from t2 where t2.c2 = t1.c1)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "exists (select c2 from t2 where t2.c2 = t1.c1)",
			expectedErrMsg:    "",
		},
		{
			name: "subquery in delete where exist",
			args: args{
				sql: "delete from t1 where exists (select c2 from t2 where t2.c2 = t1.c1)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "exists (select c2 from t2 where t2.c2 = t1.c1)",
			expectedErrMsg:    "",
		},
		{
			name: "subquery in delete where not exist",
			args: args{
				sql: "delete from t1 where not exists (select c2 from t2 where t2.c2 = t1.c1)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "not exists (select c2 from t2 where t2.c2 = t1.c1)",
			expectedErrMsg:    "",
		},
		{
			name: "foreign subquery in update where ",
			args: args{
				sql: "update t1 set c1 = '123' where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
			expectedErrMsg:    "",
		},
		{
			name: "foreign subquery in delete where ",
			args: args{
				sql: "delete from t1 where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
			},
			expectedTableName: "t1",
			expectedWhereExpr: "c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
			expectedErrMsg:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, whereExpr, _, err := parseDML(tt.args.sql)
			whereExprStr := ""
			if whereExpr != nil {
				whereExprStr = sqlparser.String(whereExpr)
			}
			if tt.expectedErrMsg != "" {
				assert.Equalf(t, "", tableName, "table name")
				assert.Equalf(t, "", whereExprStr, "where expr")
				assert.Equalf(t, tt.expectedErrMsg, err.Error(), "error message: %s", err.Error())
			} else {
				assert.Equalf(t, tt.expectedTableName, tableName, "table name")
				assert.Equalf(t, tt.expectedWhereExpr, whereExprStr, "where expr")
				assert.Equal(t, nil, err)
			}
		})
	}

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
