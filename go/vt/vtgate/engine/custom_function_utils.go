package engine

import (
	"context"
	"errors"
	"fmt"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

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
				//aliasExpr, _ := expr.(*sqlparser.AliasedExpr)
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

// GetColNamesForStar the rst is empty if sqlExprs doesn't contain *;
func (c *CustomFunctionPrimitive) GetColNamesForStar(ctx context.Context, vcursor VCursor) (map[string][]string, error) {
	send, ok := c.Input.(*Send)
	if !ok {
		return nil, errors.New("CustomFunctionPrimitive's input is not Send primitive")
	}

	schemas := make([]string, 0)
	names := make([]string, 0)
	// map alias/name/schema.name to schema.names,
	// when using * in a join case, a prefix of * can be mapped to more than one tables
	map2QualifyTableName := make(map[string][]string)

	for _, expr := range c.SentTables {
		a, s, n, err := GetNamesOfTableExpr(expr)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, s...)
		names = append(names, n...)
		for i := range a {
			qualifyName := fmt.Sprintf("%s.%s", s[i], n[i])
			if a[i] != "" {
				map2QualifyTableName[a[i]] = append(map2QualifyTableName[a[i]], qualifyName)
			}
			map2QualifyTableName[n[i]] = append(map2QualifyTableName[n[i]], qualifyName)
			map2QualifyTableName[qualifyName] = append(map2QualifyTableName[qualifyName], qualifyName)
		}
	}

	mapQualifyTableName2ColNames, err := GetColNamesForTable(ctx, send, vcursor, schemas, names)
	if err != nil {
		return nil, err
	}

	rst := make(map[string][]string)

	for _, expr := range c.Sent {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			starPrefix := sqlparser.String(star.TableName)
			if starPrefix == "" {
				// then set all cols
				for _, colNames := range mapQualifyTableName2ColNames {
					rst[starPrefix] = append(rst[starPrefix], colNames...)
				}
			} else {
				tables, exit := map2QualifyTableName[starPrefix]
				if !exit {
					return nil, fmt.Errorf("can not expand * for %v", star.TableName.Name.String())
				}
				for _, table := range tables {
					rst[starPrefix] = append(rst[starPrefix], mapQualifyTableName2ColNames[table]...)
				}
			}
		}
	}

	return rst, nil
}

// GetNamesOfTableExpr get alias, qualify, name of table expr,
// only support alias and join type table expr,
// for join, return each table's alias ("" for alias not set), qualify and name
func GetNamesOfTableExpr(tableExpr sqlparser.TableExpr) ([]string, []string, []string, error) {
	alias := make([]string, 0)
	schemas := make([]string, 0)
	names := make([]string, 0)
	if aliasTableExpr, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
		tmpTableName, ok := aliasTableExpr.Expr.(sqlparser.TableName)
		if !ok {
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: the AST has changed. This should not be possible")
		}
		alias = append(alias, aliasTableExpr.As.String())
		schemas = append(schemas, sqlparser.String(tmpTableName.Qualifier))
		names = append(names, sqlparser.String(tmpTableName.Name))
	} else if joinTableExpr, ok := tableExpr.(*sqlparser.JoinTableExpr); ok {
		a1, s1, n1, err := GetNamesOfTableExpr(joinTableExpr.LeftExpr)
		if err != nil {
			return nil, nil, nil, err
		}
		a2, s2, n2, err := GetNamesOfTableExpr(joinTableExpr.RightExpr)
		if err != nil {
			return nil, nil, nil, err
		}
		alias = append(alias, a1...)
		alias = append(alias, a2...)
		schemas = append(schemas, s1...)
		schemas = append(schemas, s2...)
		names = append(names, n1...)
		names = append(names, n2...)
	} else {
		return nil, nil, nil, errors.New("only support alias and join table expr")
	}
	return alias, schemas, names, nil
}

// GetColNamesForTable return two maps:
// mapQualifyTableName2ColNames: the key format is "qualify.name"
func GetColNamesForTable(ctx context.Context, send *Send, vcursor VCursor, tableQualify []string, tableNames []string) (map[string][]string, error) {
	if len(tableQualify) != len(tableNames) {
		return nil, errors.New("tableQualify and tableNames must have the same length")
	}
	first := true
	whereCondition := ""
	for i, tableName := range tableNames {
		if !first {
			whereCondition += " or "
		}
		whereCondition += fmt.Sprintf("(TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s')", tableQualify[i], tableName)
		first = false
	}

	fieldsQuery := fmt.Sprintf("SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE %s ORDER BY ORDINAL_POSITION", whereCondition)
	rss, err := send.Resolve(ctx, vcursor)
	if err != nil {
		return nil, err
	}
	qr, err := execShard(ctx, send, vcursor, fieldsQuery, nil, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}

	rst := make(map[string][]string)
	for _, r := range qr.Named().Rows {
		schema, _ := r.ToString("TABLE_SCHEMA")
		name, _ := r.ToString("TABLE_NAME")
		qualifyName := fmt.Sprintf("%v.%v", schema, name)
		col, _ := r.ToString("COLUMN_NAME")
		rst[qualifyName] = append(rst[qualifyName], col)
	}
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
