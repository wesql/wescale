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
	newFieldNames := make([]string, 0)
	colNamesForStar := GetColNamesForStar(c.Sent, qr.Fields)

	// we use idx to help us get the right fields for * from qr.Fields
	for _, expr := range c.Origin {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			newFieldNames = append(newFieldNames, colNamesForStar...)
		} else {
			newFieldNames = append(newFieldNames, GetSelectExprColName(expr))
		}
	}

	//build final result
	// todo newborn22, 放到remove中?
	coll := collations.TypedCollation{
		Collation:    vcursor.ConnCollation(),
		Coercibility: collations.CoerceCoercible,
		Repertoire:   collations.RepertoireUnicode,
	}

	rows := [][]sqltypes.Value{}
	for i, gotRow := range qr.Named().Rows {
		rowValues := make([]string, 0, len(newFieldNames))
		idx := 0
		for _, colExpr := range c.Origin {
			if alias, ok := colExpr.(*sqlparser.AliasedExpr); ok {
				if funcExpr, ok := alias.Expr.(*sqlparser.FuncExpr); ok {
					funcRst, err := CalFuncExpr(funcExpr, gotRow, qr.Fields, coll, bindVars, qr.Rows[i])
					if err != nil {
						return nil, err
					}
					rowValues = append(rowValues, funcRst)
					idx++
					continue
				}
			}
			if _, ok := colExpr.(*sqlparser.StarExpr); ok {
				for i := 0; i < len(colNamesForStar); i++ {
					rowValues = append(rowValues, gotRow[newFieldNames[idx]].ToString())
					idx++
				}
				continue
			}
			rowValues = append(rowValues, gotRow[newFieldNames[idx]].ToString())
			idx++
		}
		rows = append(rows, BuildVarCharRow(rowValues...))
	}

	return &sqltypes.Result{
		Fields: BuildVarCharFields(newFieldNames...),
		Rows:   rows,
	}, nil

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

// todo newborn22，是否都换成expr？ selectExpr不是epxr接口；另外还要考虑 insert select; 因此selectExpr得换
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
