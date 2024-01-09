/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"fmt"
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
			name: "Test genPKsGreaterEqualOrLessEqualStr, Single Int",
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
			name: "Test genPKsGreaterEqualOrLessEqualStr, Two INTs",
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
			name: "Test genPKsGreaterEqualOrLessEqualStr, One INT With One String",
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
			got, _ := genPKsGreaterEqualOrLessEqualStr(tt.args.pkInfos, tt.args.currentBatchStart, tt.args.greatEqual)
			assert.Equalf(t, tt.want, got, "genPKsGreaterEqualOrLessEqualStr(%v, %v, %v)", tt.args.pkInfos, tt.args.currentBatchStart, tt.args.greatEqual)
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
	batchSQL, finalWhereStr, _ := genBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL := "update t set c = 1 where (1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)"
	expectedWhereStr := "(1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)

	sql = "update t set c = 1 where 1 = 1 or 2 = 2 "
	stmt, _ = sqlparser.Parse(sql)
	whereExpr = stmt.(*sqlparser.Update).Where
	currentBatchStart = []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)}
	currentBatchEnd = []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)}
	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}}
	batchSQL, finalWhereStr, _ = genBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL = "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	expectedWhereStr = "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)

	sql = "update t set c = 1 where 1 = 1 or 2 = 2 "
	stmt, _ = sqlparser.Parse(sql)
	whereExpr = stmt.(*sqlparser.Update).Where
	currentBatchStart = []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1)}
	currentBatchEnd = []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9), sqltypes.NewInt64(9)}
	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}, {pkName: "pk3"}}
	batchSQL, finalWhereStr, _ = genBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL = "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))"
	expectedWhereStr = "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))"
	assert.Equalf(t, expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
}

func TestGenNewBatchSQLsAndCountSQLsWhenSplittingBatch(t *testing.T) {
	pkInfos := []PKInfo{{pkName: "pk1"}}
	whereStr := "where (1 = 1 and 2 = 2) and ((pk1 >= 1) and (pk1 <= 9))"
	batchSQL := fmt.Sprintf("update t set c1 = '123' %s", whereStr)
	batchCountSQL := fmt.Sprintf("select count(*) from t %s", whereStr)
	batchSQLStmt, _ := sqlparser.Parse(batchSQL)
	batchCountSQLStmt, _ := sqlparser.Parse(batchCountSQL)
	curBatchNewEnd := []sqltypes.Value{sqltypes.NewInt64(5)}
	newBatchStart := []sqltypes.Value{sqltypes.NewInt64(7)}
	expectedCurBatchSQL := "update t set c1 = '123' where 1 = 1 and 2 = 2 and (pk1 >= 1 and pk1 <= 5)"
	expectedNewBatchSQL := "update t set c1 = '123' where 1 = 1 and 2 = 2 and (pk1 >= 7 and pk1 <= 9)"
	expectedNewBatchCountSQL := "select count(*) from t where 1 = 1 and 2 = 2 and (pk1 >= 7 and pk1 <= 9)"
	curBatchSQL, newBatchSQL, newBatchCountSQL, _ := genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedCurBatchSQL, curBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedNewBatchSQL, newBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedNewBatchCountSQL, newBatchCountSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)

	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}}
	whereStr = "where (1 = 1) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	batchSQL = fmt.Sprintf("update t set c1 = '123' %s", whereStr)
	batchCountSQL = fmt.Sprintf("select count(*) from t %s", whereStr)
	batchSQLStmt, _ = sqlparser.Parse(batchSQL)
	batchCountSQLStmt, _ = sqlparser.Parse(batchCountSQL)
	curBatchNewEnd = []sqltypes.Value{sqltypes.NewInt64(5), sqltypes.NewInt64(5)}
	newBatchStart = []sqltypes.Value{sqltypes.NewInt64(7), sqltypes.NewInt64(7)}
	expectedCurBatchSQL = "update t set c1 = '123' where 1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 5 or pk1 = 5 and pk2 <= 5))"
	expectedNewBatchSQL = "update t set c1 = '123' where 1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	expectedNewBatchCountSQL = "select count(*) from t where 1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	curBatchSQL, newBatchSQL, newBatchCountSQL, _ = genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedCurBatchSQL, curBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedNewBatchSQL, newBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	assert.Equalf(t, expectedNewBatchCountSQL, newBatchCountSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)

}

func TestGetUserWhereExpr(t *testing.T) {
	sql := "update t set c1 = '123' where 1 = 1 and 2 = 2 and 3 = 3 and (pk1 >= 1 and pk1 <= 9)"
	stmt, _ := sqlparser.Parse(sql)
	userWhereExpr := getUserWhereExpr(stmt)
	userWhereStr := sqlparser.String(userWhereExpr)
	expectedUserWhereStr := "1 = 1 and 2 = 2 and 3 = 3"
	assert.Equalf(t, expectedUserWhereStr, userWhereStr, "getUserWhereExpr(%v)", stmt)
}

func TestBatchSQL(t *testing.T) {
	userWhereStr := "1 = 1 and 2 = 2"
	pkConditionStr := "(pk1 >= 1 and pk1 <= 9)"
	// 1. get batchSQL by calling genBatchSQL
	sql := fmt.Sprintf("update t set c = 1 where %s", userWhereStr)
	stmt, _ := sqlparser.Parse(sql)
	whereExpr := stmt.(*sqlparser.Update).Where
	currentBatchStart := []sqltypes.Value{sqltypes.NewInt64(1)}
	currentBatchEnd := []sqltypes.Value{sqltypes.NewInt64(9)}
	pkInfos := []PKInfo{{pkName: "pk1"}}
	batchSQL, finalWhereStr, _ := genBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	// Different from the 1st test case, here we enclose userWhereStr with parentheses because it has "or" operators.
	expectedBatchSQL := fmt.Sprintf("update t set c = 1 where (%s) and %s", userWhereStr, pkConditionStr)
	expectedWhereStr := fmt.Sprintf("(%s) and %s", userWhereStr, pkConditionStr)
	assert.Equalf(t, expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)

	batchSQLStmt, _ := sqlparser.Parse(batchSQL)
	// 2. test getUserWhereExpr base on batchSQ
	gotUserWhereExpr := getUserWhereExpr(batchSQLStmt)
	gotUserWhereStr := sqlparser.String(gotUserWhereExpr)
	expectedUserWhereStr := userWhereStr
	assert.Equalf(t, expectedUserWhereStr, gotUserWhereStr, "getUserWhereExpr(%v)", stmt)

	// 3. test getBatchSQLGreatThanAndLessThanExprNode base on batchSQL
	gtNode, lsNode := getBatchSQLGreatThanAndLessThanExprNode(batchSQLStmt)
	gtStr := sqlparser.String(gtNode)
	lsStr := sqlparser.String(lsNode)
	expectedGtStr := "pk1 >= 1"
	expectedLsStr := "pk1 <= 9"
	assert.Equalf(t, expectedGtStr, gtStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)
	assert.Equalf(t, expectedLsStr, lsStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)

	// repeat the same steps but with different args
	userWhereStr = "1 = 1 and 2 = 2 or 3 > 2 and 1 > 3 or 3 > 4 and 1 = 1"
	pkConditionStr = "((pk1 > 1 or pk1 = 1 and pk2 >= 1)  and (pk1 < 9 or pk1 = 9 and pk2 <= 9))"
	sql = fmt.Sprintf("update t set c = 1 where %s", userWhereStr)
	stmt, _ = sqlparser.Parse(sql)
	whereExpr = stmt.(*sqlparser.Update).Where
	currentBatchStart = []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)}
	currentBatchEnd = []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)}
	pkInfos = []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}}
	batchSQL, finalWhereStr, _ = genBatchSQL(sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	expectedBatchSQL = fmt.Sprintf("update t set c = 1 where %s and %s", userWhereStr, pkConditionStr)
	expectedWhereStr = fmt.Sprintf("%s and %s", userWhereStr, pkConditionStr)
	assert.Equalf(t, expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	assert.Equalf(t, expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", sql, stmt, whereExpr.Expr, currentBatchStart, currentBatchEnd, pkInfos)
	batchSQLStmt, _ = sqlparser.Parse(batchSQL)
	gotUserWhereExpr = getUserWhereExpr(batchSQLStmt)
	gotUserWhereStr = sqlparser.String(gotUserWhereExpr)
	expectedUserWhereStr = userWhereStr
	assert.Equalf(t, expectedUserWhereStr, gotUserWhereStr, "getUserWhereExpr(%v)", stmt)

	// 3. test getBatchSQLGreatThanAndLessThanExprNode base on batchSQL
	gtNode, lsNode = getBatchSQLGreatThanAndLessThanExprNode(batchSQLStmt)
	gtStr = sqlparser.String(gtNode)
	lsStr = sqlparser.String(lsNode)
	expectedGtStr = "pk1 > 1 or pk1 = 1 and pk2 >= 1"
	expectedLsStr = "pk1 < 9 or pk1 = 9 and pk2 <= 9"
	assert.Equalf(t, expectedGtStr, gtStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)
	assert.Equalf(t, expectedLsStr, lsStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)

}

func TestStripComments(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "TestStripComments-Test1",
			sql:  "select * from t1 where 1 = 1 /* comment */ and 2 = 2",
			want: "select * from t1 where 1 = 1 and 2 = 2",
		},
		{
			name: "TestStripComments-Test2",
			sql:  "select/* comment */ * from t1 where 1 = 1 and 2 = 2",
			want: "select * from t1 where 1 = 1 and 2 = 2",
		},
		{
			name: "TestStripComments-Test3",
			sql:  "update/*vt+ dml_split=true */your_table set name='brainsplit' where 1=1;",
			want: "update your_table set name='brainsplit' where 1=1;",
		},
		{
			name: "TestStripComments-Test3",
			sql:  "update             /*vt+ dml_split=true */  your_table set name='brainsplit' where 1=1;",
			want: "update your_table set name='brainsplit' where 1=1;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqlparser.StripComments(tt.sql)
			assert.Equalf(t, tt.want, got, "stripComments(%v)", tt.sql)
		})
	}
}
