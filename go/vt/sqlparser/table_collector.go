package sqlparser

import (
	"fmt"
)

type TableSchemaAndName struct {
	schema string
	name   string
}

func NewTableSchemaAndName(schema string, name string) TableSchemaAndName {
	return TableSchemaAndName{
		schema: schema,
		name:   name,
	}
}

func (t TableSchemaAndName) GetSchema() string {
	return t.schema
}

func (t TableSchemaAndName) GetName() string {
	return t.name
}

// CollectTables builds the list of required tables for all the
// tables referenced in a query.
func CollectTables(stmt Statement, defaultTableSchema string) []TableSchemaAndName {
	var tables []TableSchemaAndName
	// All Statement types myst be covered here.
	switch node := stmt.(type) {
	case *Union, *Select:
		tables = collectFromSubQuery(node, tables)
	case *Insert:
		tables = collectFromTableName(node.Table, tables)
		tables = collectFromSubQuery(node, tables)
	case *Update:
		tables = collectFromTableExprs(node.TableExprs, tables)
		tables = collectFromSubQuery(node, tables)
	case *Delete:
		tables = collectFromTableExprs(node.TableExprs, tables)
		tables = collectFromSubQuery(node, tables)
	case DDLStatement:
		for _, t := range node.AffectedTables() {
			tables = collectFromTableName(t, tables)
		}
	case
		*AlterMigration,
		*AlterDMLJob,
		*RevertMigration,
		*ShowMigrationLogs,
		*ShowThrottledApps,
		*ShowThrottlerStatus:
		tables = []TableSchemaAndName{}
	case *Flush:
		for _, t := range node.TableNames {
			tables = collectFromTableName(t, tables)
		}
	case *OtherAdmin, *CheckTable, *Kill, *CallProc, *Begin, *Commit, *Rollback,
		*Load, *Savepoint, *Release, *SRollback, *Set, *Show,
		*OtherRead, Explain, DBDDLStatement:
	// no op
	default:
		panic(fmt.Errorf("BUG: unexpected statement type: %T", node))
	}
	tables = addDefaultTableSchema(tables, defaultTableSchema)
	return removeDuplicateTables(tables)
}

func collectFromSubQuery(stmt Statement, tables []TableSchemaAndName) []TableSchemaAndName {
	_ = Walk(func(node SQLNode) (bool, error) {
		switch node := node.(type) {
		case *Select:
			tables = collectFromTableExprs(node.From, tables)
		case TableExprs:
			return false, nil
		}
		return true, nil
	}, stmt)
	return tables
}

func collectFromTableName(node TableName, tables []TableSchemaAndName) []TableSchemaAndName {
	tables = append(tables, TableSchemaAndName{
		schema: node.Qualifier.String(),
		name:   node.Name.String(),
	})
	return tables
}

func collectFromTableExprs(node TableExprs, tables []TableSchemaAndName) []TableSchemaAndName {
	for _, node := range node {
		tables = buildTableExprPermissions(node, tables)
	}
	return tables
}

func buildTableExprPermissions(node TableExpr, tables []TableSchemaAndName) []TableSchemaAndName {
	switch node := node.(type) {
	case *AliasedTableExpr:
		// An AliasedTableExpr can also be a subquery, but we should skip them here
		// because the buildSubQueryPermissions walker will catch them and extract
		// the corresponding table names.
		switch node := node.Expr.(type) {
		case TableName:
			tables = collectFromTableName(node, tables)
		case *DerivedTable:
			tables = collectFromSubQuery(node.Select, tables)
		}
	case *ParenTableExpr:
		tables = collectFromTableExprs(node.Exprs, tables)
	case *JoinTableExpr:
		tables = buildTableExprPermissions(node.LeftExpr, tables)
		tables = buildTableExprPermissions(node.RightExpr, tables)
	}
	return tables
}

func addDefaultTableSchema(tables []TableSchemaAndName, dbName string) []TableSchemaAndName {
	if dbName == "" {
		return tables
	}
	for index := range tables {
		if tables[index].schema == "" {
			tables[index].schema = dbName
		}
	}
	return tables
}

func removeDuplicateTables(tables []TableSchemaAndName) []TableSchemaAndName {
	encountered := map[TableSchemaAndName]bool{}
	var result []TableSchemaAndName

	for v := range tables {
		if encountered[tables[v]] == true {
			// Do not add duplicate.
		} else {
			encountered[tables[v]] = true
			result = append(result, tables[v])
		}
	}
	return result
}
