package engine

import (
	"errors"
	"strings"
	"unicode"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// todo newborn22 是否在这里对函数支持的类型做检查？
func GetColNameFromFuncExpr(funcExpr *sqlparser.FuncExpr) ([]sqlparser.SelectExpr, error) {
	rst := make([]sqlparser.SelectExpr, 0)
	for _, expr := range funcExpr.Exprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			alias, _ := expr.(*sqlparser.AliasedExpr)
			_, ok := alias.Expr.(*sqlparser.ColName)
			if ok {
				rst = append(rst, alias)
			} else {
				subFunc, ok := alias.Expr.(*sqlparser.FuncExpr)
				if ok {
					subExpr, err := GetColNameFromFuncExpr(subFunc)
					if err != nil {
						return nil, err
					}
					rst = append(rst, subExpr...)
				}
			}

		default:
			return nil, errors.New("not support")
		}
	}
	return rst, nil
}

func IsCustomFunctionName(fun string) bool {
	_, exist := CUSTOM_FUNCTIONS[fun]
	return exist
}

func CalFuncExpr(funcExpr *sqlparser.FuncExpr, rowValues sqltypes.RowNamedValues) (string, error) {
	// get function paras
	params := make([]string, 0, len(funcExpr.Exprs))
	// todo newborn22， 简单地支持了 literal, colname, funcExpr作为参数
	for _, para := range funcExpr.Exprs {
		alias, ok := para.(*sqlparser.AliasedExpr)
		if !ok {
			return "", errors.New("only support literal, colname and funcExpr as parameter")
		}
		switch alias.Expr.(type) {
		case *sqlparser.ColName:
			colName := alias.Expr.(*sqlparser.ColName).Name.String()
			val := rowValues[colName].ToString()
			params = append(params, val)
		case *sqlparser.Literal:
			val := sqlparser.String(alias.Expr.(*sqlparser.Literal))
			params = append(params, val)
		case *sqlparser.FuncExpr:
			// todo newborn22, 递归调用
			rst, err := CalFuncExpr(alias.Expr.(*sqlparser.FuncExpr), rowValues)
			if err != nil {
				return "", err
			}
			params = append(params, rst)
		default:
			return "", errors.New("only support literal, colname and funcExpr as parameter")
		}
	}
	// get the function
	function, _ := CUSTOM_FUNCTIONS[funcExpr.Name.String()]
	return function(params)
}

func compareStrings(s1, s2 string) bool {
	normalized1 := normalizeString(s1)
	normalized2 := normalizeString(s2)
	return normalized1 == normalized2
}

func normalizeString(s string) string {
	var builder strings.Builder
	for _, r := range s {
		if !unicode.IsSpace(r) {
			builder.WriteRune(unicode.ToLower(r))
		}
	}
	return builder.String()
}

func BuildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

func BuildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: collations.CollationUtf8ID,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

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
