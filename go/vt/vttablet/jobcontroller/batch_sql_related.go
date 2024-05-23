/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/connpool"

	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/vt/sqlparser"
)

func genPKsGreaterEqualOrLessEqualStr(pkInfos []PKInfo, currentBatchStart []sqltypes.Value, greatEqual bool) (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	prefix := ""
	// This loop handles the case for composite pks. For example,
	// if lastpk was (1,2), and the greatEqual is true, then clause would be:
	// (col1 > 1) or (col1 = 1 and col2 >= 2).
	for curCol := 0; curCol <= len(pkInfos)-1; curCol++ {
		buf.Myprintf("%s(", prefix)
		prefix = " or "
		for i, pk := range currentBatchStart[:curCol] {
			buf.Myprintf("%s = ", pkInfos[i].pkName)
			pk.EncodeSQL(buf)
			buf.Myprintf(" and ")
		}
		if curCol == len(pkInfos)-1 {
			if greatEqual {
				buf.Myprintf("%s >= ", pkInfos[curCol].pkName)
			} else {
				buf.Myprintf("%s <= ", pkInfos[curCol].pkName)
			}
		} else {
			if greatEqual {
				buf.Myprintf("%s > ", pkInfos[curCol].pkName)
			} else {
				buf.Myprintf("%s < ", pkInfos[curCol].pkName)
			}
		}
		currentBatchStart[curCol].EncodeSQL(buf)
		buf.Myprintf(")")
	}
	return buf.String(), nil
}

func genPKConditionExprByStr(greatThanPart, lessThanPart string) (sqlparser.Expr, error) {
	tmpSQL := fmt.Sprintf("select 1 where (%s) AND (%s)", greatThanPart, lessThanPart)
	tmpStmt, err := sqlparser.Parse(tmpSQL)
	if err != nil {
		return nil, err
	}
	tmpStmtSelect, ok := tmpStmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("genPKConditionExprByStr: tmpStmt is not *sqlparser.Select")
	}
	return tmpStmtSelect.Where.Expr, nil
}

func genSQLByReplaceWhereExprNode(stmt sqlparser.Statement, whereExpr sqlparser.Where) string {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		s.Where = &whereExpr
		return sqlparser.String(s)
	case *sqlparser.Delete:
		s.Where = &whereExpr
		return sqlparser.String(s)
	case *sqlparser.Select:
		// select case is for batch count SQL
		s.Where = &whereExpr
		return sqlparser.String(s)
	default:
		// the code won't reach here
		return ""
	}
}

func genBatchSQL(stmt sqlparser.Statement, whereExpr sqlparser.Expr, currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (batchSQL, finalWhereStr string, err error) {
	if stmt == nil {
		return "", "", errors.New("genBatchSQL: stmt is nil")
	}

	// 1. generate PK condition part of >=
	greatThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchStart, true)
	if err != nil {
		return "", "", err
	}

	// 2.generate generate PK condition part of <=
	lessThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchEnd, false)
	if err != nil {
		return "", "", err
	}

	// 3.Concatenate pk condition part of >= and <= to generate pk condition expr ast node
	pkConditionExpr, err := genPKConditionExprByStr(greatThanPart, lessThanPart)
	if err != nil {
		return "", "", err
	}

	// 4.Concatenate the where expr ast node of the original SQL with pkConditionExpr using AND to get the where expr of batchSQL.
	andExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: whereExpr, Right: pkConditionExpr}}
	batchSQL = genSQLByReplaceWhereExprNode(stmt, andExpr)
	finalWhereStr = sqlparser.String(andExpr.Expr)

	return batchSQL, finalWhereStr, nil
}

func genCountSQL(tableName, whereExpr string) (countSQL string) {
	countSQL = fmt.Sprintf("select count(*) as count_rows from %s where %s",
		tableName, whereExpr)
	return countSQL
}

func genBatchStartAndEndStr(currentBatchStart, currentBatchEnd []sqltypes.Value) (currentBatchStartStr string, currentBatchStartEnd string, err error) {
	prefix := ""
	for i := range currentBatchStart {
		currentBatchStartStr += prefix + currentBatchStart[i].ToString()
		currentBatchStartEnd += prefix + currentBatchEnd[i].ToString()
		prefix = ","
	}
	return currentBatchStartStr, currentBatchStartEnd, nil
}

func genExprNodeFromStr(condition string) (sqlparser.Expr, error) {
	tmpSQL := fmt.Sprintf("select 1 where %s", condition)
	tmpStmt, err := sqlparser.Parse(tmpSQL)
	if err != nil {
		return nil, err
	}
	tmpStmtSelect, _ := tmpStmt.(*sqlparser.Select)
	return tmpStmtSelect.Where.Expr, nil
}
func getBatchSQLGreatThanAndLessThanExprNode(stmt sqlparser.Statement) (greatThanExpr sqlparser.Expr, lessThanExpr sqlparser.Expr) {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		// the type switch will be ok
		andExpr, _ := s.Where.Expr.(*sqlparser.AndExpr)
		pkConditionExpr, _ := andExpr.Right.(*sqlparser.AndExpr)
		greatThanExpr = pkConditionExpr.Left
		lessThanExpr = pkConditionExpr.Right
		return greatThanExpr, lessThanExpr
	case *sqlparser.Delete:
		// the type switch will be ok
		andExpr, _ := s.Where.Expr.(*sqlparser.AndExpr)
		pkConditionExpr, _ := andExpr.Right.(*sqlparser.AndExpr)
		greatThanExpr = pkConditionExpr.Left
		lessThanExpr = pkConditionExpr.Right
		return greatThanExpr, lessThanExpr
	}
	// the code won't reach here
	return nil, nil
}

func getUserWhereExpr(stmt sqlparser.Statement) (expr sqlparser.Expr) {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		tempAndExpr, _ := s.Where.Expr.(*sqlparser.AndExpr)
		expr = tempAndExpr.Left
		return expr
	case *sqlparser.Delete:
		tempAndExpr, _ := s.Where.Expr.(*sqlparser.AndExpr)
		expr = tempAndExpr.Left
		return expr
	default:
		// the code won't reach here
		return nil
	}
}

func genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt sqlparser.Statement, curBatchNewEnd, newBatchStart []sqltypes.Value, pkInfos []PKInfo) (curBatchSQL, newBatchSQL, newBatchCountSQL string, err error) {
	// 1）Convert curBatchNewEnd and newBatchStart to greatThan and lessThan expr ast nodes
	curBatchLessThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, curBatchNewEnd, false)
	if err != nil {
		return "", "", "", err
	}
	curBatchLessThanExpr, err := genExprNodeFromStr(curBatchLessThanPart)
	if err != nil {
		return "", "", "", err
	}

	newBatchGreatThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, newBatchStart, true)
	if err != nil {
		return "", "", "", err
	}
	newBatchGreatThanExpr, err := genExprNodeFromStr(newBatchGreatThanPart)
	if err != nil {
		return "", "", "", err
	}

	// 2) Get the original batchSQL's greatThan and lessThan expr ast nodes,
	// They are respectively the greatThan part of the current batch, and the lessThan part of the new batch.
	curBatchGreatThanExpr, newBatchLessThanExpr := getBatchSQLGreatThanAndLessThanExprNode(batchSQLStmt)

	// 3) Generate batchSQL and batchCountSQL after splitting
	// 3.1) First construct the where expr ast nodes of curBatchSQL and newBatchSQL by
	// concatenating the user input where expr with PK Condition Expr with AND
	userWhereExpr := getUserWhereExpr(batchSQLStmt)
	curBatchPKConditionExpr := sqlparser.AndExpr{Left: curBatchGreatThanExpr, Right: curBatchLessThanExpr}
	newBatchPKConditionExpr := sqlparser.AndExpr{Left: newBatchGreatThanExpr, Right: newBatchLessThanExpr}
	curBatchWhereExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: userWhereExpr, Right: &curBatchPKConditionExpr}}
	newBatchWhereExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: userWhereExpr, Right: &newBatchPKConditionExpr}}

	// 3.2) Generate batchSQL by replacing the original where expr
	curBatchSQL = genSQLByReplaceWhereExprNode(batchSQLStmt, curBatchWhereExpr)
	newBatchSQL = genSQLByReplaceWhereExprNode(batchSQLStmt, newBatchWhereExpr)

	// 3.3) Similarly generate batchCountSQL
	newBatchCountSQL = genSQLByReplaceWhereExprNode(batchCountSQLStmt, newBatchWhereExpr)
	return curBatchSQL, newBatchSQL, newBatchCountSQL, nil
}

// replace selectExprs in batchCountSQLStmt with PK cols to generate selectPKsSQL
// the function will not change the original batchCountSQLStmt
func genSelectPKsSQLByBatchCountSQL(batchCountSQLStmt sqlparser.Statement, pkInfos []PKInfo) string {
	batchCountSQLStmtSelect, _ := batchCountSQLStmt.(*sqlparser.Select)
	// 根据pk信息生成select exprs
	var pkExprs []sqlparser.SelectExpr
	for _, pkInfo := range pkInfos {
		pkExprs = append(pkExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName(pkInfo.pkName)})
	}
	oldBatchCountSQLStmtSelectExprs := batchCountSQLStmtSelect.SelectExprs
	batchCountSQLStmtSelect.SelectExprs = pkExprs
	batchSplitSelectSQL := sqlparser.String(batchCountSQLStmtSelect)
	// undo the change of select exprs
	batchCountSQLStmtSelect.SelectExprs = oldBatchCountSQLStmtSelectExprs
	return batchSplitSelectSQL
}

// get the begin and end fields of the batches newly created during splitting
func getNewBatchesBeginAndEndStr(ctx context.Context, conn *connpool.DBConn, batchTable, batchID string, curBatchNewEnd, newBatchStart []sqltypes.Value) (currentBatchNewBeginStr, currentBatchNewEndStr, newBatchBeginStr, newBatchEndStr string, err error) {
	getBatchBeginAndEndSQL := fmt.Sprintf(sqlTemplateGetBatchBeginAndEnd, batchTable)
	getBatchBeginAndEndQuery, err := sqlparser.ParseAndBind(getBatchBeginAndEndSQL, sqltypes.StringBindVariable(batchID))
	if err != nil {
		return "", "", "", "", err
	}
	qr, err := conn.Exec(ctx, getBatchBeginAndEndQuery, math.MaxInt32, true)
	if err != nil {
		return "", "", "", "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", "", "", "", errors.New("can not get batch begin and end")
	}
	currentBatchNewBeginStr = qr.Named().Rows[0]["batch_begin"].ToString()
	newBatchEndStr = qr.Named().Rows[0]["batch_end"].ToString()
	currentBatchNewEndStr, newBatchBeginStr, err = genBatchStartAndEndStr(curBatchNewEnd, newBatchStart)
	if err != nil {
		return "", "", "", "", err
	}
	return currentBatchNewBeginStr, currentBatchNewEndStr, newBatchBeginStr, newBatchEndStr, nil
}
func updateBatchInfoTableEntry(ctx context.Context, conn *connpool.DBConn, batchTable string, curBatchSQL, currentBatchNewBeginStr, currentBatchNewEndStr, batchID string) (err error) {
	sqlUpdateBatchInfoTableEntry := fmt.Sprintf(sqlTemplateUpdateBatchSQL, batchTable)
	queryUpdateBatchInfoTableEntry, err := sqlparser.ParseAndBind(sqlUpdateBatchInfoTableEntry,
		sqltypes.StringBindVariable(curBatchSQL),
		sqltypes.StringBindVariable(currentBatchNewBeginStr),
		sqltypes.StringBindVariable(currentBatchNewEndStr),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, queryUpdateBatchInfoTableEntry, math.MaxInt32, false)
	if err != nil {
		return err
	}
	return nil
}

func insertBatchInfoTableEntry(ctx context.Context, conn *connpool.DBConn, batchTable, nextBatchID, newBatchSQL, newBatchCountSQL, newBatchBeginStr, newBatchEndStr string, newBatchSize int64) (err error) {
	sqlInsertBatchInfoTableEntry := fmt.Sprintf(sqlTemplateInsertBatchEntry, batchTable)
	queryInsertBatchInfoTableEntry, err := sqlparser.ParseAndBind(sqlInsertBatchInfoTableEntry,
		sqltypes.StringBindVariable(nextBatchID),
		sqltypes.StringBindVariable(newBatchSQL),
		sqltypes.StringBindVariable(newBatchCountSQL),
		sqltypes.Int64BindVariable(newBatchSize),
		sqltypes.StringBindVariable(newBatchBeginStr),
		sqltypes.StringBindVariable(newBatchEndStr))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, queryInsertBatchInfoTableEntry, math.MaxInt32, false)
	if err != nil {
		return err
	}
	return nil
}
