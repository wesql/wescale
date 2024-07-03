package engine

import (
	"errors"
	"vitess.io/vitess/go/vt/sqlparser"
)

var CUSTOM_FUNCTIONS []string = []string{"myadd"}

func HasCustomFunction(stmt sqlparser.Statement) bool {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		for _, expr := range exprs {
			switch expr.(type) {
			case *sqlparser.AliasedExpr:
				aliasExpr, _ := expr.(*sqlparser.AliasedExpr)
				funcExpr, ok := aliasExpr.Expr.(*sqlparser.FuncExpr)
				if ok {
					if IsCustomFunctionName(funcExpr.Name.String()) {
						return true
					}
				}
			}
		}
		return false
	default:
		return false
	}
}

func IsCustomFunctionName(fun string) bool {
	for _, custoFunName := range CUSTOM_FUNCTIONS {
		if custoFunName == fun {
			return true
		}
	}
	return false
}

func RemoveCustomFunction(stmt sqlparser.Statement) (string, error) {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		newExprs := make([]sqlparser.SelectExpr, 0)
		for _, expr := range exprs {
			switch expr.(type) {
			case *sqlparser.AliasedExpr:
				aliasExpr, _ := expr.(*sqlparser.AliasedExpr)
				funcExpr, ok := aliasExpr.Expr.(*sqlparser.FuncExpr)
				if ok {
					colExprs, err := GetColNameFromFuncExpr(funcExpr)
					if err != nil {
						return "", err
					}
					newExprs = append(newExprs, colExprs...)
				} else {
					newExprs = append(newExprs, expr)
				}
			default:
				newExprs = append(newExprs, expr)
			}
		}
		//newExprs = removeRedundantExpr(newExprs)
		sel.SelectExprs = newExprs
		return sqlparser.String(sel), nil
	default:
		// will not be here
		return sqlparser.String(stmt), nil
	}
}

// todo newborn22 如果有*时，是否要移除别的列？ 由于算法考虑，暂时先不用
func removeRedundantExpr(exprs []sqlparser.SelectExpr) []sqlparser.SelectExpr {
	record := make(map[string]bool)
	rst := make([]sqlparser.SelectExpr, 0)

	for _, expr := range exprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			alias, _ := expr.(*sqlparser.AliasedExpr)
			if _, exist := record[alias.ColumnName()]; !exist {
				record[alias.ColumnName()] = true
				rst = append(rst, expr)
			}
		case *sqlparser.StarExpr:
			star, _ := expr.(*sqlparser.StarExpr)
			if _, exist := record[sqlparser.String(star)]; !exist {
				record[sqlparser.String(star)] = true
				rst = append(rst, expr)
			}
		case *sqlparser.Nextval:
			nextVal, _ := expr.(*sqlparser.Nextval)
			if _, exist := record[sqlparser.String(nextVal.Expr)]; !exist {
				record[sqlparser.String(nextVal.Expr)] = true
				rst = append(rst, expr)
			}
		}
	}
	return rst
}

func InitCustomProjectionMeta(stmt sqlparser.Statement) (*CustomFunctionProjectionMeta, error) {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		return &CustomFunctionProjectionMeta{Origin: exprs}, nil
	default:
		// will not be here
		return nil, errors.New("not support")
	}
}
