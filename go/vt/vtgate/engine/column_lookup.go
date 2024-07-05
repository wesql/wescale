package engine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

type WescaleColumnLookup struct {
}

func (*WescaleColumnLookup) ColumnLookup(col *sqlparser.ColName) (int, error) {
	// todo newborn22, 计算正确的offset
	return 0, nil
}

func (*WescaleColumnLookup) CollationForExpr(expr sqlparser.Expr) collations.ID {
	// todo newborn22, 从其他地方获得collation
	mySQLVersion := "8.0.0"
	collationEnv := collations.NewEnvironment(mySQLVersion)
	return collationEnv.LookupByName("utf8mb4_bin").ID()
}

func (*WescaleColumnLookup) DefaultCollation() collations.ID {
	// todo newborn22, 从其他地方获得collation
	mySQLVersion := "8.0.0"
	collationEnv := collations.NewEnvironment(mySQLVersion)
	return collationEnv.LookupByName("utf8mb4_bin").ID()
}
