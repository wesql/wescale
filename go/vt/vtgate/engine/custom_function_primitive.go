package engine

import (
	"context"
	"errors"
	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type CustomFunctionPrimitive struct {
	Input  Primitive
	Origin sqlparser.SelectExprs
	Sent   sqlparser.SelectExprs
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
	colNamesForStar := GetColNamesForStar(c.Sent, qr.Fields)

	for _, expr := range c.Origin {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			finalFieldNames = append(finalFieldNames, colNamesForStar...)
		} else {
			finalFieldNames = append(finalFieldNames, GetSelectExprColName(expr))
		}
	}

	// build executable exprs to get final result
	// here, we should use offset variable to get right offset for each colName expr,
	// because the name in SelectExprs is not equal to the name in qr.Fields,
	// for example, '1+1' in qr.Fields is 'vt1+vt1' in SelectExprs
	coll := collations.TypedCollation{
		Collation:    vcursor.ConnCollation(),
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}
	finalExprs := make([]evalengine.Expr, 0, len(finalFieldNames))
	offset := 0

	for _, colExpr := range c.Origin {
		if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
			if funcExpr, ok := alias.Expr.(*sqlparser.FuncExpr); ok {
				callExpr, err := InitCallExprForFuncExpr(funcExpr, &offset, coll)
				if err != nil {
					return nil, err
				}
				finalExprs = append(finalExprs, callExpr)
				continue
			}
		}
		if _, ok := colExpr.(*sqlparser.StarExpr); ok {
			for i := 0; i < len(colNamesForStar); i++ {
				col := evalengine.NewColumn(offset, coll)
				finalExprs = append(finalExprs, col)
				offset++
			}
			continue
		}
		finalExprs = append(finalExprs, evalengine.NewColumn(offset, coll))
		offset++
	}

	//build final result
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	env.Fields = BuildVarCharFields(finalFieldNames...)
	var resultRows []sqltypes.Row
	for _, row := range qr.Rows {
		resultRow := make(sqltypes.Row, 0, len(finalExprs))
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

	// todo newborn22, 放到remove中? expr由于 *的存在，在remove中做不了，因为没查之前不知道*对应几列

	//rows := [][]sqltypes.Value{}
	//for i, gotRow := range qr.Named().Rows {
	//	rowValues := make([]string, 0, len(finalFieldNames))
	//	idx := 0
	//	for _, colExpr := range c.Origin {
	//		if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
	//			if funcExpr, ok := alias.Expr.(*sqlparser.FuncExpr); ok {
	//				funcRst, err := CalFuncExpr(funcExpr, gotRow, qr.Fields, coll, bindVars, qr.Rows[i])
	//				if err != nil {
	//					return nil, err
	//				}
	//				rowValues = append(rowValues, funcRst)
	//				idx++
	//				continue
	//			}
	//		}
	//		if _, ok := colExpr.(*sqlparser.StarExpr); ok {
	//			for i := 0; i < len(colNamesForStar); i++ {
	//				rowValues = append(rowValues, gotRow[finalFieldNames[idx]].ToString())
	//				idx++
	//			}
	//			continue
	//		}
	//		rowValues = append(rowValues, gotRow[finalFieldNames[idx]].ToString())
	//		idx++
	//	}
	//	rows = append(rows, BuildVarCharRow(rowValues...))
	//}
	//
	//return &sqltypes.Result{
	//	Fields: BuildVarCharFields(finalFieldNames...),
	//	Rows:   rows,
	//}, nil

	//return qr, nil
}

// TryStreamExecute implements the Primitive interface
func (c *CustomFunctionPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return errors.New("not implemented yet")
}

// GetFields implements the Primitive interface
func (c *CustomFunctionPrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, errors.New("not implemented yet")
}

func (c *CustomFunctionPrimitive) addFields(env *evalengine.ExpressionEnv, qr *sqltypes.Result) error {
	return errors.New("not implemented yet")
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
