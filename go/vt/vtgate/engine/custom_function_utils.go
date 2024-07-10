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
				lookup := &evalengine.CustomFunctionLookup{}
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

func (c *CustomFunctionPrimitive) RewriteQueryForCustomFunction(stmt sqlparser.Statement) (string, error) {
	switch stmt.(type) {
	case *sqlparser.Select:
		sel, _ := stmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		newExprs := make([]sqlparser.SelectExpr, 0)
		collationExprs := make([]sqlparser.SelectExpr, 0)
		for _, expr := range exprs {
			switch expr.(type) {
			case *sqlparser.StarExpr:
				newExprs = append(newExprs, expr)
				c.TransferColName = append(c.TransferColName, true)

				// for star expr, we can't call collation(*), but we still take place, to make sure the len of collationExprs is the same as newExprs
				collationFunc := &sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("collation"), Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral("1")}}}
				collationExprs = append(collationExprs, &sqlparser.AliasedExpr{Expr: collationFunc})

			case *sqlparser.AliasedExpr:
				//aliasExpr, _ := expr.(*sqlparser.AliasedExpr)
				lookup := &evalengine.CustomFunctionLookup{}
				_, err := evalengine.Translate(expr.(*sqlparser.AliasedExpr).Expr, lookup)
				if err != nil {
					return "", err
				}
				if lookup.HasCustomFunction {
					for _, param := range lookup.FuncParams {
						newExprs = append(newExprs, &sqlparser.AliasedExpr{Expr: param})
						collationFunc := &sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("collation"), Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: param}}}
						collationExprs = append(collationExprs, &sqlparser.AliasedExpr{Expr: collationFunc})
					}
					c.TransferColName = append(c.TransferColName, false)
				} else {
					newExprs = append(newExprs, expr)
					c.TransferColName = append(c.TransferColName, true)

					collationFunc := &sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("collation"), Exprs: []sqlparser.SelectExpr{expr}}
					collationExprs = append(collationExprs, &sqlparser.AliasedExpr{Expr: collationFunc})
				}

			case *sqlparser.Nextval:
				return "", errors.New("next value type select expr is not supported in custom function framework")
			}
		}

		sel.SelectExprs = append(newExprs, collationExprs...)
		if len(sel.SelectExprs) == 0 {
			// need to query table to get rows, so we fill literal 1 as place holder.
			// if tables in the sql is not assigned, wescale will add 'dual' as table,
			// we can identify that case and do some quick calculation on custom functions without sending a query to mysql,
			// but it will make mistakes if user create a table named 'dual'.
			sel.SelectExprs = append(sel.SelectExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral("1")})
		}
		return sqlparser.String(sel), nil
	default:
		// will not be here
		return "", errors.New("RewriteQueryForCustomFunction: sql type not supported")
	}
}

func (c *CustomFunctionPrimitive) QuickCalculate() (*sqltypes.Result, error) {
	// build final field and expr
	finalFieldNames := make([]string, 0)
	finalExprs := make([]evalengine.Expr, 0, len(finalFieldNames))

	for _, expr := range c.Origin {
		if alias, ok := expr.(*sqlparser.AliasedExpr); ok {
			finalFieldNames = append(finalFieldNames, alias.ColumnName())
			// there should be no any info about columns required, so we can use any lookup here
			e, err := evalengine.Translate(alias.Expr, &evalengine.CustomFunctionLookup{})
			if err != nil {
				return nil, err
			}
			finalExprs = append(finalExprs, e)
		} else {
			return nil, fmt.Errorf("select expr type %v is not expected here", sqlparser.String(expr))
		}
	}

	// build final result
	env := evalengine.EnvWithBindVars(nil, collations.Unknown)
	env.Fields = BuildVarCharFields(finalFieldNames...)
	var resultRows []sqltypes.Row

	// there should be only 1 row in result
	resultRow := make(sqltypes.Row, 0, 1)
	env.Row = nil
	for _, exp := range finalExprs {
		result, err := env.Evaluate(exp)
		if err != nil {
			return nil, err
		}
		resultRow = append(resultRow, result.Value())
	}
	resultRows = append(resultRows, resultRow)

	return &sqltypes.Result{
		Fields: BuildVarCharFields(finalFieldNames...),
		Rows:   resultRows,
	}, nil
}

type ColInfo struct {
	ColName     string
	CollationID collations.ID
}

// GetColNamesForStar the rst is empty if sqlExprs doesn't contain *;
func (c *CustomFunctionPrimitive) GetColNamesForStar(ctx context.Context, vcursor VCursor) (map[string][]ColInfo, error) {
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

	mapQualifyTableName2ColInfo, err := GetColNamesForTable(ctx, send, vcursor, schemas, names)
	if err != nil {
		return nil, err
	}

	rst := make(map[string][]ColInfo)

	for _, expr := range c.Sent {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			starPrefix := sqlparser.String(star.TableName)
			if starPrefix == "" {
				// then set all cols
				for _, colInfos := range mapQualifyTableName2ColInfo {
					rst[starPrefix] = append(rst[starPrefix], colInfos...)
				}
			} else {
				tables, exit := map2QualifyTableName[starPrefix]
				if !exit {
					return nil, fmt.Errorf("can not expand * for %v", star.TableName.Name.String())
				}
				for _, table := range tables {
					rst[starPrefix] = append(rst[starPrefix], mapQualifyTableName2ColInfo[table]...)
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

// GetColNamesForTable the key format is "qualify.name"
func GetColNamesForTable(ctx context.Context, send *Send, vcursor VCursor, tableQualify []string, tableNames []string) (map[string][]ColInfo, error) {
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

	fieldsQuery := fmt.Sprintf("SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE %s ORDER BY ORDINAL_POSITION", whereCondition)
	rss, err := send.Resolve(ctx, vcursor)
	if err != nil {
		return nil, err
	}
	qr, err := execShard(ctx, send, vcursor, fieldsQuery, nil, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}

	rst := make(map[string][]ColInfo)
	for _, r := range qr.Named().Rows {
		schema, _ := r.ToString("TABLE_SCHEMA")
		name, _ := r.ToString("TABLE_NAME")
		qualifyName := fmt.Sprintf("%v.%v", schema, name)
		col, _ := r.ToString("COLUMN_NAME")
		collation, _ := r.ToString("COLLATION_NAME")
		rst[qualifyName] = append(rst[qualifyName], ColInfo{ColName: col, CollationID: collations.CollationNameToID[collation]})
	}
	return rst, nil
}

// todo newborn22，是否都换成expr？ selectExpr不是epxr接口；另外还要考虑 insert select; 因此selectExpr得换
func InitCustomFunctionPrimitive(originQuery string, originStmt sqlparser.Statement) (*CustomFunctionPrimitive, error) {
	switch originStmt.(type) {
	case *sqlparser.Select:
		sel, _ := originStmt.(*sqlparser.Select)
		exprs := sel.SelectExprs
		return &CustomFunctionPrimitive{Origin: exprs, OriginStmt: originStmt, OriginQuery: originQuery}, nil
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
