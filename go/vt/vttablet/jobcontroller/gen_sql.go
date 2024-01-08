/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func GenPKsGreaterEqualOrLessEqualStr(pkInfos []PKInfo, currentBatchStart []sqltypes.Value, greatEqual bool) (string, error) {
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

func GenPKConditionExprByStr(greatThanPart, lessThanPart string) (sqlparser.Expr, error) {
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

func GenSQLByReplaceWhereExprNode(stmt sqlparser.Statement, whereExpr sqlparser.Where) string {
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

func ReplaceWhereExprNode(stmt sqlparser.Statement, whereExpr sqlparser.Where) sqlparser.Statement {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		s.Where = &whereExpr
		return s
	case *sqlparser.Delete:
		s.Where = &whereExpr
		return s
	default:
		// the code won't reach here
		return nil
	}
}

// todo newborn22 对参数进行调整
func GenBatchSQL(sql string, stmt sqlparser.Statement, whereExpr sqlparser.Expr, currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (batchSQL, finalWhereStr string, err error) {
	// 1. 生成>=的部分
	greatThanPart, err := GenPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchStart, true)
	if err != nil {
		return "", "", err
	}

	// 2.生成<=的部分
	lessThanPart, err := GenPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchEnd, false)
	if err != nil {
		return "", "", err
	}

	// 3.将pk>= and pk <= 拼接起来并生成相应的condition expr ast node
	pkConditionExpr, err := GenPKConditionExprByStr(greatThanPart, lessThanPart)
	if err != nil {
		return "", "", err
	}

	// 4.将原本sql stmt中的where expr ast node用AND拼接上pkConditionExpr，作为batchSQL的where expr ast node
	// 4.1先生成新的condition ast node
	andExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: whereExpr, Right: pkConditionExpr}}
	batchSQL = GenSQLByReplaceWhereExprNode(stmt, andExpr)
	finalWhereStr = sqlparser.String(andExpr.Expr)

	return batchSQL, finalWhereStr, nil
}

// todo newbon22 删除
// todo newborn22 batchSQL和batchCountSQL对浮点数进行拦截，可能在获得pkInfo时就进行拦截。
// 拆分列所支持的类型需要满足以下条件：
// 1.在sql中可以正确地使用between或>=,<=进行比较运算，且没有精度问题。
// 2.可以转换成go中的int64，float64或string三种类型之一，且转换后，在golang中的比较规则和mysql中的比较规则相同
func GenCountSQLOld(tableSchema, tableName, wherePart string, currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (countSQL string, err error) {
	// 1. 生成>=的部分
	greatThanPart, err := GenPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchStart, true)
	if err != nil {
		return "", err
	}

	// 2.生成<=的部分
	lessThanPart, err := GenPKsGreaterEqualOrLessEqualStr(pkInfos, currentBatchEnd, false)
	if err != nil {
		return "", err
	}

	// 3.将各部分拼接成最终的countSQL
	countSQL = fmt.Sprintf("select count(*) as count_rows from %s.%s where (%s) and ((%s) and (%s))",
		tableSchema, tableName, wherePart, greatThanPart, lessThanPart)

	return countSQL, nil
}

// todo newborn22 batchSQL和batchCountSQL对浮点数进行拦截，可能在获得pkInfo时就进行拦截。
// 拆分列所支持的类型需要满足以下条件：
// 1.在sql中可以正确地使用between或>=,<=进行比较运算，且没有精度问题。
// 2.可以转换成go中的int64，float64或string三种类型之一，且转换后，在golang中的比较规则和mysql中的比较规则相同
func GenCountSQL(tableSchema, tableName, whereExpr string) (countSQL string) {
	countSQL = fmt.Sprintf("select count(*) as count_rows from %s.%s where %s)",
		tableSchema, tableName, whereExpr)
	return countSQL
}
