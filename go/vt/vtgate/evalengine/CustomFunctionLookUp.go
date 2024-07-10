package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

// CustomFunctionLookup is used in wescale custom function framework, there are two ways to use it:
// 1. without setting ColOffset, CollationIdx and RowContainsCollation, the look up can be used as a Translate parameter to check whether t
// an expr has a custom function, if has, look up will set all ColName type parameter of function in FuncParams.
// 2. if set ColOffset, CollationIdx and RowContainsCollation, it can be used to translate a sqlparser.ColName expr to engine.Column expr
type CustomFunctionLookup struct {
	HasCustomFunction    bool
	FuncParams           []*sqlparser.ColName
	ColOffset            *int
	CollationIdx         *int
	RowContainsCollation sqltypes.Row
}

func (c *CustomFunctionLookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
	if c.HasCustomFunction {
		c.FuncParams = append(c.FuncParams, col)
	}
	if c.ColOffset != nil {
		tmp := *c.ColOffset
		*c.ColOffset++
		return tmp, nil
	}
	return -1, nil
}

func (c *CustomFunctionLookup) CollationForExpr(_ sqlparser.Expr) collations.ID {
	if c.CollationIdx != nil && c.RowContainsCollation != nil {
		tmp := *c.CollationIdx
		collationName := c.RowContainsCollation[tmp].ToString()
		collationID, exist := collations.CollationNameToID[collationName]
		if !exist {
			return collations.Unknown
		}
		*c.CollationIdx++
		return collationID
	}

	return collations.Unknown
}

func (c *CustomFunctionLookup) DefaultCollation() collations.ID {
	return collations.Unknown
}
