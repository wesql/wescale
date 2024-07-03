package engine

import (
	"errors"
	"strconv"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CustomFunction func([]string) (string, error)

var CUSTOM_FUNCTIONS map[string]CustomFunction = map[string]CustomFunction{"myadd": myadd}

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
	_, exist := CUSTOM_FUNCTIONS[fun]
	return exist
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

func myadd(parameters []string) (string, error) {
	if len(parameters) != 2 {
		return "", errors.New("myadd: should have two int parameters")
	}
	num1, err := strconv.Atoi(parameters[0])
	if err != nil {
		return "", errors.New("myadd: first parameter should be int")
	}
	num2, err := strconv.Atoi(parameters[1])
	if err != nil {
		return "", errors.New("myadd: second parameter should be int")
	}

	return strconv.Itoa(num1 + num2), nil
}
