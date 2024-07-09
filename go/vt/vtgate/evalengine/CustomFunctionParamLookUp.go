package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CustomFunctionParamLookup struct {
	HasCustomFunction bool
	FuncParams        []*sqlparser.ColName
	ColOffset         *int
}

func (c *CustomFunctionParamLookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
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

func (c *CustomFunctionParamLookup) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return collations.Unknown
}

func (c *CustomFunctionParamLookup) DefaultCollation() collations.ID {
	return collations.Unknown
}
