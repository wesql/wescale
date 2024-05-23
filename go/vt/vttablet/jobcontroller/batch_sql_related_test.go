/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/vt/sqlparser"
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
	type args struct {
		sql                                string
		currentBatchStart, currentBatchEnd []sqltypes.Value
		pkInfos                            []PKInfo
	}
	tests := []struct {
		name             string
		args             args
		expectedBatchSQL string
		expectedWhereStr string
	}{
		{
			name: "Single Int PK",
			args: args{
				sql:               "update t set c = 1 where 1 = 1 or 2 = 2 and 3 = 3",
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1)},
				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9)},
				pkInfos:           []PKInfo{{pkName: "pk1"}},
			},
			expectedBatchSQL: "update t set c = 1 where (1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)",
			expectedWhereStr: "(1 = 1 or 2 = 2 and 3 = 3) and (pk1 >= 1 and pk1 <= 9)",
		},
		{
			name: "Two Int PK",
			args: args{
				sql:               "update t set c = 1 where 1 = 1 or 2 = 2",
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)},
				pkInfos:           []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
			},
			expectedBatchSQL: "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedWhereStr: "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
		},
		{
			name: "Three Int PK",
			args: args{
				sql:               "update t set c = 1 where 1 = 1 or 2 = 2",
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9), sqltypes.NewInt64(9)},
				pkInfos:           []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}, {pkName: "pk3"}},
			},
			expectedBatchSQL: "update t set c = 1 where (1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))",
			expectedWhereStr: "(1 = 1 or 2 = 2) and ((pk1 > 1 or pk1 = 1 and pk2 > 1 or pk1 = 1 and pk2 = 1 and pk3 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 < 9 or pk1 = 9 and pk2 = 9 and pk3 <= 9))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := sqlparser.Parse(tt.args.sql)
			var whereAst *sqlparser.Where
			switch s := stmt.(type) {
			case *sqlparser.Update:
				whereAst = s.Where
			case *sqlparser.Delete:
				whereAst = s.Where
			default:
				t.Fatalf("unsupported statement type: %T", stmt)
			}
			batchSQL, finalWhereStr, _ := genBatchSQL(stmt, whereAst.Expr, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v)", tt.args.sql, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v)", tt.args.sql, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
		})
	}
}

func TestGenNewBatchSQLsAndCountSQLsWhenSplittingBatch(t *testing.T) {
	type args struct {
		batchSQL                      string
		batchCountSQL                 string
		curBatchNewEnd, newBatchStart []sqltypes.Value
		pkInfos                       []PKInfo
	}
	tests := []struct {
		name                     string
		args                     args
		expectedCurBatchSQL      string
		expectedNewBatchSQL      string
		expectedNewBatchCountSQL string
	}{
		{
			name: "Single Int PK",
			args: args{
				batchSQL:       "update t set c1 = '123' where (1 = 1 and 2 = 2) and ((pk1 >= 1) and (pk1 <= 9))",
				batchCountSQL:  "select count(*) from t where (1 = 1 and 2 = 2) and ((pk1 >= 1) and (pk1 <= 9))",
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
				pkInfos:        []PKInfo{{pkName: "pk1"}},
			},
			expectedCurBatchSQL:      "update t set c1 = '123' where 1 = 1 and 2 = 2 and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:      "update t set c1 = '123' where 1 = 1 and 2 = 2 and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL: "select count(*) from t where 1 = 1 and 2 = 2 and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "Two Int PK",
			args: args{
				batchSQL:       "update t set c1 = '123' where (1 = 1) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
				batchCountSQL:  "select count(*) from t where (1 = 1) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5), sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7), sqltypes.NewInt64(7)},
				pkInfos:        []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
			},
			expectedCurBatchSQL:      "update t set c1 = '123' where 1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 5 or pk1 = 5 and pk2 <= 5))",
			expectedNewBatchSQL:      "update t set c1 = '123' where 1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedNewBatchCountSQL: "select count(*) from t where 1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchSQLStmt, _ := sqlparser.Parse(tt.args.batchSQL)
			batchCountSQLStmt, _ := sqlparser.Parse(tt.args.batchCountSQL)
			curBatchSQL, newBatchSQL, newBatchCountSQL, _ := genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedCurBatchSQL, curBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedNewBatchSQL, newBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedNewBatchCountSQL, newBatchCountSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart, tt.args.pkInfos)
		})
	}
}

func TestGetUserWhereExpr(t *testing.T) {
	type args struct {
		sql string
	}
	test := []struct {
		name                 string
		args                 args
		expectedUserWhereStr string
	}{
		{
			name: "and",
			args: args{
				sql: "update t set c1 = '123' where 1 = 1 and 2 = 2 and 3 = 3 and (pk1 >= 1 and pk1 <= 9)",
			},
			expectedUserWhereStr: "1 = 1 and 2 = 2 and 3 = 3",
		},
	}
	for _, tt := range test {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := sqlparser.Parse(tt.args.sql)
			userWhereExpr := getUserWhereExpr(stmt)
			userWhereStr := sqlparser.String(userWhereExpr)
			assert.Equalf(t, tt.expectedUserWhereStr, userWhereStr, "getUserWhereExpr(%v)", stmt)
		})
	}
}

func TestBatchSQL(t *testing.T) {

	type args struct {
		sql                                string
		userWhereStr                       string
		currentBatchStart, currentBatchEnd []sqltypes.Value
		pkInfos                            []PKInfo
	}
	tests := []struct {
		name             string
		args             args
		expectedBatchSQL string
		expectedWhereStr string
		expectedGtStr    string
		expectedLsStr    string
	}{
		{
			name: "Single Int PK",
			args: args{
				sql:               "update t set c = 1 where 1 = 1 and 2 = 2",
				userWhereStr:      "1 = 1 and 2 = 2",
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(5)},
				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(7)},
				pkInfos:           []PKInfo{{pkName: "pk1"}},
			},
			expectedBatchSQL: "update t set c = 1 where 1 = 1 and 2 = 2 and (pk1 >= 5 and pk1 <= 7)",
			expectedWhereStr: "1 = 1 and 2 = 2 and (pk1 >= 5 and pk1 <= 7)",
			expectedGtStr:    "pk1 >= 5",
			expectedLsStr:    "pk1 <= 7",
		},
		{
			name: "Two Int PK",
			args: args{
				sql:               "update t set c = 1 where 1 = 1 and 2 = 2 or 3 > 2 and 1 > 3 or 3 > 4 and 1 = 1",
				userWhereStr:      "1 = 1 and 2 = 2 or 3 > 2 and 1 > 3 or 3 > 4 and 1 = 1",
				currentBatchStart: []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
				currentBatchEnd:   []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)},
				pkInfos:           []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
			},
			expectedBatchSQL: "update t set c = 1 where (1 = 1 and 2 = 2 or 3 > 2 and 1 > 3 or 3 > 4 and 1 = 1) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedWhereStr: "(1 = 1 and 2 = 2 or 3 > 2 and 1 > 3 or 3 > 4 and 1 = 1) and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedGtStr:    "pk1 > 1 or pk1 = 1 and pk2 >= 1",
			expectedLsStr:    "pk1 < 9 or pk1 = 9 and pk2 <= 9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// 1. get batchSQL by calling genBatchSQL
			stmt, _ := sqlparser.Parse(tt.args.sql)
			var whereAst *sqlparser.Where
			switch s := stmt.(type) {
			case *sqlparser.Update:
				whereAst = s.Where
			case *sqlparser.Delete:
				whereAst = s.Where
			default:
				t.Fatalf("unsupported statement type: %T", stmt)
			}

			batchSQL, finalWhereStr, _ := genBatchSQL(stmt, whereAst.Expr, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedBatchSQL, batchSQL, "genBatchSQL(%v, %v, %v,%v,%v, %v)", tt.args.sql, stmt, whereAst.Expr, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedWhereStr, finalWhereStr, "genBatchSQL(%v, %v, %v,%v,%v, %v)", tt.args.sql, stmt, whereAst.Expr, tt.args.currentBatchStart, tt.args.currentBatchEnd, tt.args.pkInfos)
			batchSQLStmt, _ := sqlparser.Parse(batchSQL)

			// 2. test getUserWhereExpr base on batchSQ
			gotUserWhereExpr := getUserWhereExpr(batchSQLStmt)
			gotUserWhereStr := sqlparser.String(gotUserWhereExpr)
			expectedUserWhereStr := tt.args.userWhereStr
			assert.Equalf(t, expectedUserWhereStr, gotUserWhereStr, "getUserWhereExpr(%v)", stmt)

			// 3. test getBatchSQLGreatThanAndLessThanExprNode base on batchSQL
			gtNode, lsNode := getBatchSQLGreatThanAndLessThanExprNode(batchSQLStmt)
			gtStr := sqlparser.String(gtNode)
			lsStr := sqlparser.String(lsNode)
			assert.Equalf(t, tt.expectedGtStr, gtStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)
			assert.Equalf(t, tt.expectedLsStr, lsStr, "getBatchSQLGreatThanAndLessThanExprNode(%v)", batchSQLStmt)
		})
	}

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

func TestGenSelectPKsSQL(t *testing.T) {
	type args struct {
		batchCountSQL string
		pkInfos       []PKInfo
	}
	tests := []struct {
		name        string
		args        args
		expectedSQL string
	}{
		{
			name: "basic test",
			args: args{
				batchCountSQL: "select count(*) from t where 1 = 1",
				pkInfos:       []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
			},
			expectedSQL: "select pk1, pk2 from t where 1 = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchCountSQLStmt, _ := sqlparser.Parse(tt.args.batchCountSQL)
			gotSQL := genSelectPKsSQLByBatchCountSQL(batchCountSQLStmt, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedSQL, gotSQL, "genSelectPKsSQLByBatchCountSQL(%v,%v)", batchCountSQLStmt, tt.args.pkInfos)
			// batchCountSQLStmt should not be changed
			assert.Equalf(t, tt.args.batchCountSQL, sqlparser.String(batchCountSQLStmt), "genSelectPKsSQLByBatchCountSQL(%v,%v)", batchCountSQLStmt, tt.args.pkInfos)
		})
	}

}

func TestParseDML2GenBatchSQL(t *testing.T) {
	type args struct {
		dmlSQL                        string
		pkInfos                       []PKInfo
		curBatchStart, curBatchEnd    []sqltypes.Value
		curBatchNewEnd, newBatchStart []sqltypes.Value
	}
	tests := []struct {
		name                                                                  string
		args                                                                  args
		expectedTableName                                                     string
		expectedWhereStr                                                      string
		expectedParseDMLErr                                                   error
		expectedSelectPksSQL                                                  string
		expectedGenBatchSQLErr                                                error
		expectedBatchSQL, expectedBatchCountSQL                               string
		expectedGenNewBatchSQLErr                                             error
		expectedCurBatchNewSQL, expectedNewBatchSQL, expectedNewBatchCountSQL string
	}{
		{
			name: "single int PK",
			args: args{
				dmlSQL:         "update t as mytable set c = '123' where c1 = 1",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "t as mytable",
			expectedWhereStr:          "c1 = 1",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from t as mytable where c1 = 1 order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update t as mytable set c = '123' where c1 = 1 and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from t as mytable where c1 = 1 and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update t as mytable set c = '123' where c1 = 1 and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update t as mytable set c = '123' where c1 = 1 and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from t as mytable where c1 = 1 and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "two int PKs",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c = '123' where c1 = 1",
				pkInfos:        []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5), sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7), sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "c1 = 1",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1,pk2 from mydb.t as mytable where c1 = 1 order by pk1,pk2",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= 1) and (pk1 < 5 or pk1 = 5 and pk2 <= 5))",
			expectedNewBatchSQL:       "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where c1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= 7) and (pk1 < 9 or pk1 = 9 and pk2 <= 9))",
		},
		{
			name: "composite pk types: int and datetime",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c = '123' where c1 = 1",
				pkInfos:        []PKInfo{{pkName: "pk1"}, {pkName: "pk2"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewDatetime("2024-01-01 12:34:56")},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9), sqltypes.NewDatetime("2024-01-01 12:34:56")},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5), sqltypes.NewDatetime("2024-01-01 12:34:56")},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7), sqltypes.NewDatetime("2024-01-01 12:34:56")},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "c1 = 1",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1,pk2 from mydb.t as mytable where c1 = 1 order by pk1,pk2",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= '2024-01-01 12:34:56') and (pk1 < 9 or pk1 = 9 and pk2 <= '2024-01-01 12:34:56'))",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= '2024-01-01 12:34:56') and (pk1 < 9 or pk1 = 9 and pk2 <= '2024-01-01 12:34:56'))",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 1 or pk1 = 1 and pk2 >= '2024-01-01 12:34:56') and (pk1 < 5 or pk1 = 5 and pk2 <= '2024-01-01 12:34:56'))",
			expectedNewBatchSQL:       "update mydb.t as mytable set c = '123' where c1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= '2024-01-01 12:34:56') and (pk1 < 9 or pk1 = 9 and pk2 <= '2024-01-01 12:34:56'))",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where c1 = 1 and ((pk1 > 7 or pk1 = 7 and pk2 >= '2024-01-01 12:34:56') and (pk1 < 9 or pk1 = 9 and pk2 <= '2024-01-01 12:34:56'))",
		},
		{
			name: "complex where condition",
			args: args{
				dmlSQL: "update mydb.t as mytable set c = '123' where " +
					"(c1 = 'v1' or c2 > 100) and " +
					"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
					"c5 is null and c6 between 10 and 100",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName: "mydb.t as mytable",
			expectedWhereStr: "(c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100",
			expectedParseDMLErr: nil,
			expectedSelectPksSQL: "select pk1 from mydb.t as mytable where " +
				"(c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100" +
				" order by pk1",
			expectedGenBatchSQLErr: nil,
			expectedBatchSQL: "update mydb.t as mytable set c = '123' where " +
				"((c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100)" +
				" and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL: "select count(*) as count_rows from mydb.t as mytable where " +
				"((c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100)" +
				" and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL: "update mydb.t as mytable set c = '123' where " +
				"((c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100)" +
				" and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL: "update mydb.t as mytable set c = '123' where " +
				"((c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100)" +
				" and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL: "select count(*) as count_rows from mydb.t as mytable where " +
				"((c1 = 'v1' or c2 > 100) and " +
				"(c3 like 'abc%' and c4 in ('a', 'b', 'c')) or " +
				"c5 is null and c6 between 10 and 100)" +
				" and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "subquery in where condition",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c1 = '123' where c2 in (select c2 from t2)",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "c2 in (select c2 from t2)",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from mydb.t as mytable where c2 in (select c2 from t2) order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c1 = '123' where c2 in (select c2 from t2) and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where c2 in (select c2 from t2) and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c1 = '123' where c2 in (select c2 from t2) and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update mydb.t as mytable set c1 = '123' where c2 in (select c2 from t2) and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where c2 in (select c2 from t2) and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "subquery in set",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c1 = (select c1 from t2 limit 1) where 1 = 1",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "1 = 1",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from mydb.t as mytable where 1 = 1 order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c1 = (select c1 from t2 limit 1) where 1 = 1 and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where 1 = 1 and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c1 = (select c1 from t2 limit 1) where 1 = 1 and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update mydb.t as mytable set c1 = (select c1 from t2 limit 1) where 1 = 1 and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where 1 = 1 and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "subquery in where exsit",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c1 = '123' where exists (select c2 from t2 where t2.c2 = mytable.c1)",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "exists (select c2 from t2 where t2.c2 = mytable.c1)",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from mydb.t as mytable where exists (select c2 from t2 where t2.c2 = mytable.c1) order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c1 = '123' where exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c1 = '123' where exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update mydb.t as mytable set c1 = '123' where exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "subquery in where not exsit",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c1 = '123' where not exists (select c2 from t2 where t2.c2 = mytable.c1)",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "not exists (select c2 from t2 where t2.c2 = mytable.c1)",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from mydb.t as mytable where not exists (select c2 from t2 where t2.c2 = mytable.c1) order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c1 = '123' where not exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where not exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c1 = '123' where not exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update mydb.t as mytable set c1 = '123' where not exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where not exists (select c2 from t2 where t2.c2 = mytable.c1) and (pk1 >= 7 and pk1 <= 9)",
		},
		{
			name: "foreign subquery in where",
			args: args{
				dmlSQL:         "update mydb.t as mytable set c1 = '123' where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
				pkInfos:        []PKInfo{{pkName: "pk1"}},
				curBatchStart:  []sqltypes.Value{sqltypes.NewInt64(1)},
				curBatchEnd:    []sqltypes.Value{sqltypes.NewInt64(9)},
				curBatchNewEnd: []sqltypes.Value{sqltypes.NewInt64(5)},
				newBatchStart:  []sqltypes.Value{sqltypes.NewInt64(7)},
			},
			expectedTableName:         "mydb.t as mytable",
			expectedWhereStr:          "c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key)",
			expectedParseDMLErr:       nil,
			expectedSelectPksSQL:      "select pk1 from mydb.t as mytable where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) order by pk1",
			expectedGenBatchSQLErr:    nil,
			expectedBatchSQL:          "update mydb.t as mytable set c1 = '123' where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) and (pk1 >= 1 and pk1 <= 9)",
			expectedBatchCountSQL:     "select count(*) as count_rows from mydb.t as mytable where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) and (pk1 >= 1 and pk1 <= 9)",
			expectedGenNewBatchSQLErr: nil,
			expectedCurBatchNewSQL:    "update mydb.t as mytable set c1 = '123' where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) and (pk1 >= 1 and pk1 <= 5)",
			expectedNewBatchSQL:       "update mydb.t as mytable set c1 = '123' where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) and (pk1 >= 7 and pk1 <= 9)",
			expectedNewBatchCountSQL:  "select count(*) as count_rows from mydb.t as mytable where c2 = (select max(c2) from t2 where t2.foreign_key = t1.primary_key) and (pk1 >= 7 and pk1 <= 9)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1.parse dmlsql and verify
			tableName, whereExpr, stmt, err := parseDML(tt.args.dmlSQL)
			if tt.expectedParseDMLErr == nil {
				assert.Nil(t, err)
			} else {
				assert.Equalf(t, tt.expectedParseDMLErr.Error(), err.Error(), "parseDML(%v)", tt.args.dmlSQL)
			}
			whereStr := sqlparser.String(whereExpr)
			assert.Equalf(t, tt.expectedTableName, tableName, "parseDML(%v)", tt.args.dmlSQL)
			assert.Equalf(t, tt.expectedWhereStr, whereStr, "parseDML(%v)", tt.args.dmlSQL)

			// 2.get selectPksSQL
			selectPksSQL := sprintfSelectPksSQL(tableName, whereStr, tt.args.pkInfos)
			assert.Equalf(t, tt.expectedSelectPksSQL, selectPksSQL, "sprintfSelectPksSQL(%v,%v)", tableName, whereStr)

			// 3.get batchSQL and batchCountSQL
			batchSQL, finalWhereStr, err := genBatchSQL(stmt, whereExpr, tt.args.curBatchStart, tt.args.curBatchEnd, tt.args.pkInfos)
			batchCountSQL := genCountSQL(tableName, finalWhereStr)
			if tt.expectedGenBatchSQLErr == nil {
				assert.Nil(t, err)
			} else {
				assert.Equalf(t, tt.expectedGenBatchSQLErr.Error(), err.Error(), "genBatchSQL(%v,%v,%v,%v)", stmt, whereExpr, tt.args.curBatchStart, tt.args.curBatchEnd)
			}
			assert.Equalf(t, tt.expectedBatchSQL, batchSQL, "genBatchSQL(%v,%v,%v,%v)", stmt, whereExpr, tt.args.curBatchStart, tt.args.curBatchEnd)
			assert.Equalf(t, tt.expectedBatchCountSQL, batchCountSQL, "genBatchSQL(%v,%v,%v,%v	)", stmt, whereExpr, tt.args.curBatchStart, tt.args.curBatchEnd)

			// 4.split batch
			batchSQLStmt, _ := sqlparser.Parse(batchSQL)
			batchCountSQLStmt, _ := sqlparser.Parse(batchCountSQL)
			curBatchNewSQL, newBatchSQL, newBatchCountSQL, err := genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart, tt.args.pkInfos)
			if tt.expectedGenNewBatchSQLErr == nil {
				assert.Nil(t, err)
			} else {
				assert.Equalf(t, tt.expectedGenNewBatchSQLErr.Error(), err.Error(), "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart)
			}
			assert.Equalf(t, tt.expectedCurBatchNewSQL, curBatchNewSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart)
			assert.Equalf(t, tt.expectedNewBatchSQL, newBatchSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart)
			assert.Equalf(t, tt.expectedNewBatchCountSQL, newBatchCountSQL, "genNewBatchSQLsAndCountSQLsWhenSplittingBatch(%v,%v,%v,%v)", batchSQLStmt, batchCountSQLStmt, tt.args.curBatchNewEnd, tt.args.newBatchStart)
		})
	}
}
