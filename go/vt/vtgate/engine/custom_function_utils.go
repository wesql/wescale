package engine

import (
	"errors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// todo newborn22，
// 函数目前支持所有的表达式作为参数，会交由mysql进行计算，wescale只负责将mysql计算结果作为函数输入的参数
func GetParaExprFromFuncExpr(funcExpr *sqlparser.FuncExpr) ([]sqlparser.SelectExpr, error) {
	rst := make([]sqlparser.SelectExpr, 0)
	for _, expr := range funcExpr.Exprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			alias, _ := expr.(*sqlparser.AliasedExpr)
			subFunc, ok := alias.Expr.(*sqlparser.FuncExpr)
			if ok {
				subExpr, err := GetParaExprFromFuncExpr(subFunc)
				if err != nil {
					return nil, err
				}
				rst = append(rst, subExpr...)
			} else {
				rst = append(rst, expr)
			}
		case *sqlparser.Nextval:
			rst = append(rst, expr)
		case *sqlparser.StarExpr:
			return nil, errors.New("not support")
		}
	}
	return rst, nil
}

func IsCustomFunctionName(fun string) bool {
	_, exist := evalengine.CustomFunctions[fun]
	return exist
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
					colExprs, err := GetParaExprFromFuncExpr(funcExpr)
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

func GetSelectExprColName(expr sqlparser.SelectExpr) string {
	return sqlparser.String(expr)
}

// GetColNamesForStar the rst is empty if sqlExprs doesn't contain *;
// todo newborn22 0705 多表*
func GetColNamesForStar(sqlExprs sqlparser.SelectExprs, resultField []*querypb.Field) []string {
	numberOfColNamesForStar := len(resultField) - len(sqlExprs) + 1
	rst := make([]string, 0, numberOfColNamesForStar)

	if len(resultField) == len(sqlExprs) {
		return rst
	}

	idx := 0
	findStar := false
	for _, expr := range sqlExprs {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			findStar = true
			break
		}
		idx++
	}
	if findStar {
		for i := 0; i < numberOfColNamesForStar; i++ {
			rst = append(rst, resultField[idx].Name)
			idx++
		}
	}

	return rst
}

func InitCallExprForFuncExpr(expr *sqlparser.FuncExpr, offset *int, coll collations.TypedCollation) (*evalengine.CallExpr, error) {
	// todo newborn22 0705 mysql func
	f, exist := evalengine.CustomFunctions[expr.Name.String()]
	if !exist {
		return nil, errors.New("function not found in builtin funcitons")
	}
	args := make([]evalengine.Expr, 0)
	// todo newborn22, 0705 translate
	for _, para := range expr.Exprs {
		if alias, ok := para.(*sqlparser.AliasedExpr); ok {
			if subFunc, ok := alias.Expr.(*sqlparser.FuncExpr); ok {
				subFuncCallExpr, err := InitCallExprForFuncExpr(subFunc, offset, coll)
				if err != nil {
					return nil, err
				}
				args = append(args, subFuncCallExpr)
				continue
			}
		}

		colExpr := evalengine.NewColumn(*offset, coll)
		*offset++
		args = append(args, colExpr)
	}

	rst := &evalengine.CallExpr{F: f, Arguments: args}
	return rst, nil
}

// todo newborn22，是否都换成expr？ selectExpr不是epxr接口；另外还要考虑 insert select; 因此selectExpr得换
func InitCustomFunctionPrimitive(stmt sqlparser.Statement) (*CustomFunctionPrimitive, error) {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		return &CustomFunctionPrimitive{Origin: exprs}, nil
	default:
		// will not be here
		return nil, errors.New("InitCustomFunctionPrimitive not support stmt type besides select")
	}
}

func (c *CustomFunctionPrimitive) SetSentSelectExprs(stmt sqlparser.Statement) error {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		c.Sent = sel.SelectExprs
		return nil
	default:
		return errors.New("SetSentSelectExprs not support stmt type besides select")
	}
}
