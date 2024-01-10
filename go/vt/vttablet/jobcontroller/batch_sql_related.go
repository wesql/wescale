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

	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
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
		// 针对batchCountSQL
		s.Where = &whereExpr
		return sqlparser.String(s)
	default:
		// the code won't reach here
		return ""
	}
}

// todo newborn22 对参数进行调整
func genBatchSQL(sql string, stmt sqlparser.Statement, whereExpr sqlparser.Expr, currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (batchSQL, finalWhereStr string, err error) {
	// 1. 生成>=的部分
	greatThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchStart, true)
	if err != nil {
		return "", "", err
	}

	// 2.生成<=的部分
	lessThanPart, err := genPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchEnd, false)
	if err != nil {
		return "", "", err
	}

	// 3.将pk>= and pk <= 拼接起来并生成相应的condition expr ast node
	pkConditionExpr, err := genPKConditionExprByStr(greatThanPart, lessThanPart)
	if err != nil {
		return "", "", err
	}

	// 4.将原本sql stmt中的where expr ast node用AND拼接上pkConditionExpr，作为batchSQL的where expr ast node
	// 4.1先生成新的condition ast node
	andExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: whereExpr, Right: pkConditionExpr}}
	batchSQL = genSQLByReplaceWhereExprNode(stmt, andExpr)
	finalWhereStr = sqlparser.String(andExpr.Expr)

	return batchSQL, finalWhereStr, nil
}

// todo newborn22 batchSQL和batchCountSQL对浮点数进行拦截，可能在获得pkInfo时就进行拦截。
// 拆分列所支持的类型需要满足以下条件：
// 1.在sql中可以正确地使用between或>=,<=进行比较运算，且没有精度问题。
// 2.可以转换成go中的int64，float64或string三种类型之一，且转换后，在golang中的比较规则和mysql中的比较规则相同
func genCountSQL(tableSchema, tableName, whereExpr string) (countSQL string) {
	countSQL = fmt.Sprintf("select count(*) as count_rows from %s.%s where %s",
		tableSchema, tableName, whereExpr)
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
	// 1) 将curBatchNewEnd和newBatchStart转换成<=和>=的字符串，然后将字符串转成expr ast node
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

	// 2) 通过parser，获得原先batchSQL的greatThan和lessThan的expr ast node
	curBatchGreatThanExpr, newBatchLessThanExpr := getBatchSQLGreatThanAndLessThanExprNode(batchSQLStmt)

	// 3) 生成拆batchSQL和batchCountSQL
	// 3.1) 先构建curBatchSQL和newBatchSQL的where expr ast node：将用户输入的where expr与上PK Condition Expr
	userWhereExpr := getUserWhereExpr(batchSQLStmt)
	curBatchPKConditionExpr := sqlparser.AndExpr{Left: curBatchGreatThanExpr, Right: curBatchLessThanExpr}
	newBatchPKConditionExpr := sqlparser.AndExpr{Left: newBatchGreatThanExpr, Right: newBatchLessThanExpr}
	curBatchWhereExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: userWhereExpr, Right: &curBatchPKConditionExpr}}
	newBatchWhereExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: userWhereExpr, Right: &newBatchPKConditionExpr}}

	// 3.2) 替换原先batchSQL的where expr来生成batchSQL
	curBatchSQL = genSQLByReplaceWhereExprNode(batchSQLStmt, curBatchWhereExpr)
	newBatchSQL = genSQLByReplaceWhereExprNode(batchSQLStmt, newBatchWhereExpr)

	// 3.3) 同理生成batchCountSQL
	newBatchCountSQL = genSQLByReplaceWhereExprNode(batchCountSQLStmt, newBatchWhereExpr)
	return curBatchSQL, newBatchSQL, newBatchCountSQL, nil
}

// replace selectExprs in batchCountSQLStmt with PK cols to generate selectPKsSQL
// the function will not change the original batchCountSQLStmt
func genSelectPKsSQL(batchCountSQLStmt sqlparser.Statement, pkInfos []PKInfo) string {
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
