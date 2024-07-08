package engine

import (
	"context"
	"errors"
	"fmt"

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
				lookup := &evalengine.CustomFunctionParamLookup{}
				_, err := evalengine.Translate(expr.(*sqlparser.AliasedExpr).Expr, lookup)
				if err != nil {
					return false
				}
				if lookup.HasCustomFunction {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

func (c *CustomFunctionPrimitive) RemoveCustomFunction(stmt sqlparser.Statement) (string, error) {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		newExprs := make([]sqlparser.SelectExpr, 0)
		for _, expr := range exprs {
			switch expr.(type) {
			case *sqlparser.StarExpr:
				newExprs = append(newExprs, expr)
				c.TransferColName = append(c.TransferColName, true)

			case *sqlparser.AliasedExpr:
				lookup := &evalengine.CustomFunctionParamLookup{}
				_, err := evalengine.Translate(expr.(*sqlparser.AliasedExpr).Expr, lookup)
				if err != nil {
					return "", err
				}
				if lookup.HasCustomFunction {
					for _, param := range lookup.FuncParams {
						newExprs = append(newExprs, &sqlparser.AliasedExpr{Expr: param, As: param.Name})
					}
					c.TransferColName = append(c.TransferColName, false)
				} else {
					newExprs = append(newExprs, expr)
					c.TransferColName = append(c.TransferColName, true)
				}

			case *sqlparser.Nextval:
				return "", errors.New("next value type select expr is not supported in custom function framework")
			}
		}

		sel.SelectExprs = newExprs
		return sqlparser.String(sel), nil
	default:
		// will not be here
		return "", errors.New("RemoveCustomFunction: sql type not supported")
	}
}

func GetSelectExprColName(expr sqlparser.SelectExpr) string {
	return sqlparser.String(expr)
}

// GetColNamesForStar the rst is empty if sqlExprs doesn't contain *;
// todo newborn22 0705 多表*
func (c *CustomFunctionPrimitive) GetColNamesForStar(ctx context.Context, vcursor VCursor) (map[string][]string, error) {
	send, ok := c.Input.(*Send)
	if !ok {
		return nil, errors.New("CustomFunctionPrimitive's input is not send")
	}

	rst := make(map[string][]string)

	for _, expr := range c.Sent {
		if star, ok := expr.(*sqlparser.StarExpr); ok {

			tableName := star.TableName.Name.String()
			if tableName == "" {
				tableName = sqlparser.String(c.SentTables)
			} else {
				// todo newborn22, support more table expr types
				asMap := make(map[string]string)
				for _, t := range c.SentTables {
					if alias, ok := t.(*sqlparser.AliasedTableExpr); ok {
						tableAlias := alias.As.String()

						if tableAlias == "" {
							asMap[sqlparser.String(alias.Expr)] = sqlparser.String(alias.Expr)
						} else {
							asMap[tableAlias] = sqlparser.String(alias.Expr)
						}
					} else {
						return nil, errors.New("only support alias table expr")
					}
				}
				var exist bool
				tableName, exist = asMap[tableName]
				if !exist {
					return nil, fmt.Errorf("table %v not found", tableName)
				}
			}

			fieldsQuery := fmt.Sprintf("select * from %s limit 1", tableName)
			rss, err := send.Resolve(ctx, vcursor)
			if err != nil {
				return nil, err
			}
			qr, err := execShard(ctx, send, vcursor, fieldsQuery, nil, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
			if err != nil {
				return nil, err
			}
			names := make([]string, 0, len(qr.Fields))
			for _, field := range qr.Fields {
				names = append(names, field.Name)
			}
			rst[star.TableName.Name.String()] = names
		}
	}

	return rst, nil
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

func (c *CustomFunctionPrimitive) SetSentExprs(stmt sqlparser.Statement) error {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		c.Sent = sel.SelectExprs
		c.SentTables = sel.From
		return nil
	default:
		return errors.New("SetSentExprs not support stmt type besides select")
	}
}
