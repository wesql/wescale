package engine

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type CustomFunctionPrimitive struct {
	Input Primitive

	OriginStmt        sqlparser.Statement
	OriginSelectExprs sqlparser.SelectExprs

	SentSelectExprs sqlparser.SelectExprs
	SentTables      sqlparser.TableExprs

	TransferColName []bool
}

// RouteType implements the Primitive interface
func (c *CustomFunctionPrimitive) RouteType() string {
	return c.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (c *CustomFunctionPrimitive) GetKeyspaceName() string {
	return c.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (c *CustomFunctionPrimitive) GetTableName() string {
	return c.Input.GetTableName()
}

// TryExecute implements the Primitive interface
func (c *CustomFunctionPrimitive) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(ctx, c.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// build final field
	finalFieldNames := make([]string, 0)
	colInfoForStar, err := c.GetColNamesForStar(ctx, vcursor)
	if err != nil {
		return nil, err
	}

	// ensure that there is at least one row, so we can get the collation info
	if len(qr.Rows) == 0 {
		return &sqltypes.Result{
			Fields: BuildVarCharFields(finalFieldNames...),
			Rows:   nil,
		}, nil
	}

	for _, expr := range c.OriginSelectExprs {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			starPrefix := sqlparser.String(star.TableName)
			colNames := make([]string, 0)
			for _, colInfo := range colInfoForStar[starPrefix] {
				colNames = append(colNames, colInfo.ColName)
			}
			finalFieldNames = append(finalFieldNames, colNames...)
		} else if alias, ok := expr.(*sqlparser.AliasedExpr); ok {
			finalFieldNames = append(finalFieldNames, alias.ColumnName())
		}
	}

	// build executable exprs to get final result
	// here, we should use offset variable to get right offset for each colName expr,
	// because the name in SelectExprs is not equal to the name in qr.Fields,
	// for example, '1+1' in qr.Fields is 'vt1+vt1' in SelectExprs
	finalExprs := make([]evalengine.Expr, 0, len(finalFieldNames))

	// if send an expr to mysql, also send a collation related to the expr,
	// so len(c.Sent)>>1 is the number of collation we send,
	// collationIdx begin from len(qr.Fields) - len(c.Sent)>>1 in qr.Fields
	collationIdx := len(qr.Fields) - len(c.SentSelectExprs)>>1

	colOffset := 0
	lookup := &evalengine.CustomFunctionLookup{ColOffset: &colOffset, CollationIdx: &collationIdx, RowContainsCollation: qr.Rows[0]}

	for i, colExpr := range c.OriginSelectExprs {
		if star, ok := colExpr.(*sqlparser.StarExpr); ok {
			starPrefix := sqlparser.String(star.TableName)
			colInfos := colInfoForStar[starPrefix]

			for _, info := range colInfos {
				coll := collations.TypedCollation{
					Collation:    info.CollationID,
					Coercibility: collations.CoerceImplicit,
					Repertoire:   collations.RepertoireUnicode,
				}
				col := evalengine.NewColumn(colOffset, coll)
				colOffset++
				finalExprs = append(finalExprs, col)
			}
			// one place holder collation for star expr
			collationIdx++
			continue
		}

		if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
			if c.TransferColName[i] {
				collationName := qr.Rows[0][collationIdx].ToString()
				collationID, exist := collations.CollationNameToID[collationName]
				if !exist {
					return nil, fmt.Errorf("collation %s for %s not found", collationName, sqlparser.String(alias.Expr))
				}

				coll := collations.TypedCollation{
					Collation:    collationID,
					Coercibility: collations.CoerceImplicit,
					Repertoire:   collations.RepertoireUnicode,
				}
				col := evalengine.NewColumn(colOffset, coll)
				colOffset++
				collationIdx++
				finalExprs = append(finalExprs, col)
			} else {
				expr, err := evalengine.Translate(alias.Expr, lookup)
				if err != nil {
					return nil, err
				}
				finalExprs = append(finalExprs, expr)
			}

		} else {
			return nil, fmt.Errorf("not support select expr type %v", sqlparser.String(colExpr))
		}

	}

	// build final result
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	env.Fields = BuildVarCharFields(finalFieldNames...)
	var resultRows []sqltypes.Row
	for _, row := range qr.Rows {
		resultRow := make(sqltypes.Row, 0, len(qr.Rows))
		env.Row = row
		for _, exp := range finalExprs {
			result, err := env.Evaluate(exp)
			if err != nil {
				return nil, err
			}
			resultRow = append(resultRow, result.Value())
		}
		resultRows = append(resultRows, resultRow)
	}

	return &sqltypes.Result{
		Fields: BuildVarCharFields(finalFieldNames...),
		Rows:   resultRows,
	}, nil
}

// TryStreamExecute implements the Primitive interface
func (c *CustomFunctionPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return vcursor.StreamExecutePrimitive(ctx, c.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		// build final field
		finalFieldNames := make([]string, 0)
		colInfoForStar, err := c.GetColNamesForStar(ctx, vcursor)
		if err != nil {
			return err
		}

		// ensure that there is at least one row, so we can get the collation info
		if len(qr.Rows) == 0 {
			return nil
		}

		for _, expr := range c.OriginSelectExprs {
			if star, ok := expr.(*sqlparser.StarExpr); ok {
				starPrefix := sqlparser.String(star.TableName)
				colNames := make([]string, 0)
				for _, colInfo := range colInfoForStar[starPrefix] {
					colNames = append(colNames, colInfo.ColName)
				}
				finalFieldNames = append(finalFieldNames, colNames...)
			} else if alias, ok := expr.(*sqlparser.AliasedExpr); ok {
				finalFieldNames = append(finalFieldNames, alias.ColumnName())
			}
		}

		// build executable exprs to get final result
		// here, we should use offset variable to get right offset for each colName expr,
		// because the name in SelectExprs is not equal to the name in qr.Fields,
		// for example, '1+1' in qr.Fields is 'vt1+vt1' in SelectExprs
		finalExprs := make([]evalengine.Expr, 0, len(finalFieldNames))

		// if send an expr to mysql, also send a collation related to the expr,
		// so len(c.Sent)>>1 is the number of collation we send,
		// collationIdx begin from len(qr.Fields) - len(c.Sent)>>1 in qr.Fields
		collationIdx := len(qr.Fields) - len(c.SentSelectExprs)>>1

		colOffset := 0
		lookup := &evalengine.CustomFunctionLookup{ColOffset: &colOffset, CollationIdx: &collationIdx, RowContainsCollation: qr.Rows[0]}

		for i, colExpr := range c.OriginSelectExprs {
			if star, ok := colExpr.(*sqlparser.StarExpr); ok {
				starPrefix := sqlparser.String(star.TableName)
				colInfos := colInfoForStar[starPrefix]

				for _, info := range colInfos {
					coll := collations.TypedCollation{
						Collation:    info.CollationID,
						Coercibility: collations.CoerceImplicit,
						Repertoire:   collations.RepertoireUnicode,
					}
					col := evalengine.NewColumn(colOffset, coll)
					colOffset++
					finalExprs = append(finalExprs, col)
				}
				// one place holder collation for star expr
				collationIdx++
				continue
			}

			if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
				if c.TransferColName[i] {
					collationName := qr.Rows[0][collationIdx].ToString()
					collationID, exist := collations.CollationNameToID[collationName]
					if !exist {
						return fmt.Errorf("collation %s for %s not found", collationName, sqlparser.String(alias.Expr))
					}

					coll := collations.TypedCollation{
						Collation:    collationID,
						Coercibility: collations.CoerceImplicit,
						Repertoire:   collations.RepertoireUnicode,
					}
					col := evalengine.NewColumn(colOffset, coll)
					colOffset++
					collationIdx++
					finalExprs = append(finalExprs, col)
				} else {
					expr, err := evalengine.Translate(alias.Expr, lookup)
					if err != nil {
						return err
					}
					finalExprs = append(finalExprs, expr)
				}

			} else {
				return fmt.Errorf("not support select expr type %v", sqlparser.String(colExpr))
			}

		}

		// build final result
		env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
		env.Fields = BuildVarCharFields(finalFieldNames...)
		var resultRows []sqltypes.Row
		for _, row := range qr.Rows {
			resultRow := make(sqltypes.Row, 0, len(qr.Rows))
			env.Row = row
			for _, exp := range finalExprs {
				result, err := env.Evaluate(exp)
				if err != nil {
					return err
				}
				resultRow = append(resultRow, result.Value())
			}
			resultRows = append(resultRows, resultRow)
		}

		qr.Fields = BuildVarCharFields(finalFieldNames...)
		qr.Rows = resultRows

		return callback(qr)
	})
}

// GetFields implements the Primitive interface
func (c *CustomFunctionPrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// build final field
	finalFieldNames := make([]string, 0)
	colInfoForStar, err := c.GetColNamesForStar(ctx, vcursor)
	if err != nil {
		return nil, err
	}

	for _, expr := range c.OriginSelectExprs {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			starPrefix := sqlparser.String(star.TableName)
			colNames := make([]string, 0)
			for _, colInfo := range colInfoForStar[starPrefix] {
				colNames = append(colNames, colInfo.ColName)
			}
			finalFieldNames = append(finalFieldNames, colNames...)
		} else if alias, ok := expr.(*sqlparser.AliasedExpr); ok {
			finalFieldNames = append(finalFieldNames, alias.ColumnName())
		}
	}

	return &sqltypes.Result{
		Fields: BuildVarCharFields(finalFieldNames...),
	}, nil
}

// Inputs implements the Primitive interface
func (c *CustomFunctionPrimitive) Inputs() []Primitive {
	return []Primitive{c.Input}
}

// description implements the Primitive interface
func (c *CustomFunctionPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "CustomFunctionPrimitive",
	}
}

// NeedsTransaction implements the Primitive interface
func (c *CustomFunctionPrimitive) NeedsTransaction() bool {
	return false
}
