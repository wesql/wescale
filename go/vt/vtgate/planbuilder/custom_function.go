package planbuilder

import "vitess.io/vitess/go/vt/sqlparser"

var CUSTOM_FUNCTIONS []string = []string{"myadd"}

func hasCustomFunction(exprs sqlparser.SelectExprs) bool {
	for _, expr := range exprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			aliasExpr, _ := expr.(*sqlparser.AliasedExpr)
			funcExpr, ok := aliasExpr.Expr.(*sqlparser.FuncExpr)
			if ok {
				if IsCustomFunctionName(funcExpr.Name.String()) {
					return true
				}
			}
		}
	}
	return false
}

func IsCustomFunctionName(fun string) bool {
	for _, custoFunName := range CUSTOM_FUNCTIONS {
		if custoFunName == fun {
			return true
		}
	}
	return false
}
