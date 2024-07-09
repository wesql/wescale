package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type CustomFunctionPrimitive struct {
	Input           Primitive
	Origin          sqlparser.SelectExprs
	TransferColName []bool
	Sent            sqlparser.SelectExprs
	SentTables      sqlparser.TableExprs
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
	colNamesForStar, err := c.GetColNamesForStar(ctx, vcursor)
	if err != nil {
		return nil, err
	}

	for _, expr := range c.Origin {
		if star, ok := expr.(*sqlparser.StarExpr); ok {
			finalFieldNames = append(finalFieldNames, colNamesForStar[star.TableName.Name.String()]...)
		} else {
			finalFieldNames = append(finalFieldNames, GetSelectExprColName(expr))
		}
	}

	// build executable exprs to get final result
	// here, we should use offset variable to get right offset for each colName expr,
	// because the name in SelectExprs is not equal to the name in qr.Fields,
	// for example, '1+1' in qr.Fields is 'vt1+vt1' in SelectExprs

	// todo newbon22 0705 每个列的collation
	coll := collations.TypedCollation{
		Collation:    vcursor.ConnCollation(),
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}
	finalExprs := make([]evalengine.Expr, 0, len(finalFieldNames))

	offsetMap := make(map[string]int)
	for i, field := range qr.Fields {
		offsetMap[strings.ToLower(field.Name)] = i
	}
	lookup := &evalengine.CustomFunctionParamLookup{ColOffsets: offsetMap}

	for i, colExpr := range c.Origin {

		if star, ok := colExpr.(*sqlparser.StarExpr); ok {
			colNames := make([]string, 0)
			if star.TableName.Name.String() == "" {
				for _, cols := range colNamesForStar {
					colNames = append(colNames, cols...)
				}
			} else {
				colNames = colNamesForStar[star.TableName.Name.String()]
			}

			for _, col := range colNames {
				offset, exit := offsetMap[strings.ToLower(col)]
				if !exit {
					return nil, fmt.Errorf("not found column offset for %v", col)
				}
				col := evalengine.NewColumn(offset, coll)
				finalExprs = append(finalExprs, col)
			}
			continue
		}

		if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
			if c.TransferColName[i] {
				offset, exit := offsetMap[strings.ToLower(sqlparser.String(alias.Expr))]
				if !exit {
					return nil, fmt.Errorf("not found column offset for %v", sqlparser.String(alias.Expr))
				}
				col := evalengine.NewColumn(offset, coll)
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
}

// TryStreamExecute implements the Primitive interface
// todo newbon22 0705
func (c *CustomFunctionPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return errors.New("not implemented yet")
}

// GetFields implements the Primitive interface
func (c *CustomFunctionPrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, errors.New("not implemented yet")
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
