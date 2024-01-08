/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGenPKsGreaterEqualOrLessEqual(t *testing.T) {
	type args struct {
		pkInfos           []PKInfo
		currentBatchStart []sqltypes.Value
		greatEqual        bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test GenPKsGreaterEqualOrLessEqualStr, Single Int",
			args: args{
				pkInfos: []PKInfo{
					{pkName: "a"},
				},
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1)},
				greatEqual:        true,
			},
			want: "(a >= 1)",
		},
		{
			name: "Test GenPKsGreaterEqualOrLessEqualStr, Two INTs",
			args: args{
				pkInfos: []PKInfo{
					{pkName: "a"},
					{pkName: "b"},
				},
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
				greatEqual:        true,
			},
			want: "(a > 1) or (a = 1 and b >= 2)",
		},
		{
			name: "Test GenPKsGreaterEqualOrLessEqualStr, One INT With One String",
			args: args{
				pkInfos: []PKInfo{
					{pkName: "a"},
					{pkName: "b"},
				},
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewTimestamp("1704630977")},
				greatEqual:        false,
			},
			want: "(a < 1) or (a = 1 and b <= '1704630977')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := GenPKsGreaterEqualOrLessEqualStr(tt.args.pkInfos, tt.args.currentBatchStart, tt.args.greatEqual)
			assert.Equalf(t, tt.want, got, "GenPKsGreaterEqualOrLessEqualStr(%v, %v, %v)", tt.args.pkInfos, tt.args.currentBatchStart, tt.args.greatEqual)
		})
	}
}

func TestGenBatchSQL(t *testing.T) {
	sql := "update t set c = 1 where 1 = 1 or 2 = 2 and 3 = 3"
	stmt, _ := sqlparser.Parse(sql)
	whereExpr := stmt.(*sqlparser.Update).Where
	currentBatchStart := []sqltypes.Value{sqltypes.NewInt64(1)}
	currentBatchEnd := []sqltypes.Value{sqltypes.NewInt64(9)}
	pkInfos := []PKInfo{{pkName: "pk1"}}
	batchSQL, finalWhereStr, _ := GenBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL := "update t set c = 1 where (1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)"
	expectedWhereStr := "(1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)

	sql = "update t set c = 1 where 1 = 1 or 2 = 2 "
	stmt, _ = sqlparser.Parse(sql)
	whereExpr = stmt.(*sqlparser.Update).Where
	currentBatchStart = []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)}
	currentBatchEnd = []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)}
	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}}
	batchSQL, finalWhereStr, _ = GenBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL = "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	expectedWhereStr = "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)

	sql = "update t set c = 1 where 1 = 1 or 2 = 2 "
	stmt, _ = sqlparser.Parse(sql)
	whereExpr = stmt.(*sqlparser.Update).Where
	currentBatchStart = []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1)}
	currentBatchEnd = []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9), sqltypes.NewInt64(9)}
	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}, {pkName: "pk3"}}
	batchSQL, finalWhereStr, _ = GenBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL = "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))"
	expectedWhereStr = "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "GenBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
}

//func TestGenCountSQL_old(t *testing.T) {
//	type args struct {
//		tableSchema       string
//		tableName         string
//		wherePart         string
//		currentBatchStart []sqltypes.Value
//		currentBatchEnd   []sqltypes.Value
//		pkInfos           []PKInfo
//	}
//	tests := []struct {
//		name string
//		args args
//		want string
//	}{
//		{
//			name: "test_db",
//			args: args{
//				tableSchema:       "test_db",
//				tableName:         "test_table",
//				wherePart:         "id = 1",
//				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1)},
//				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9)},
//				pkInfos:           []PKInfo{{pkName: "pk1"}},
//			},
//			want: "select count(*) as count_rows from test_db.test_table where (id = 1) and (((pk1 >= 1)) and ((pk1 <= 9)))",
//		},
//		{
//			name: "test_db",
//			args: args{
//				tableSchema:       "test_db",
//				tableName:         "test_table",
//				wherePart:         "id = 1",
//				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
//				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)},
//				pkInfos:           []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
//			},
//			want: "select count(*) as count_rows from test_db.test_table where (id = 1) and (((pk1 > 1) or (pk1 = 1 and pk2 >= 1)) and ((pk1 < 9) or (pk1 = 9 and pk2 <= 9)))",
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, _ := GenCountSQL(tt.args.tableSchema, tt.args.tableName, tt.args.wherePart, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
//			assert.Equalf(t, tt.want, got, "GenCountSQL(%v,%v,%v,%v,%v,%v)", tt.args.tableSchema, tt.args.tableName, tt.args.wherePart, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
//		})
//	}
//}
