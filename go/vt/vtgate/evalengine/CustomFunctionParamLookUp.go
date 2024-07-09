package evalengine

import (
	"strings"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CustomFunctionParamLookup struct {
	HasCustomFunction bool
	FuncParams        []*sqlparser.ColName
	ColOffsets        map[string]int
	ColOffset         int
}

func (c *CustomFunctionParamLookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
	if c.HasCustomFunction {
		c.FuncParams = append(c.FuncParams, col)
	}
	if off, exist := c.ColOffsets[strings.ToLower(col.Name.String())]; exist {
		return off, nil
	}
	return -1, nil
}

func (c *CustomFunctionParamLookup) CollationForExpr(_ sqlparser.Expr) collations.ID {
	return collations.Unknown
}

func (c *CustomFunctionParamLookup) DefaultCollation() collations.ID {
	return collations.Unknown
}
