/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func GetTableSchemaAndNameFromDdlStmt(schemaOfSession string, ddlStmt sqlparser.DDLStatement) ([]TableSchemaAndName, error) {
	result := make([]TableSchemaAndName, 0, 1)
	switch ddlStmt := ddlStmt.(type) {
	case *sqlparser.CreateTable, *sqlparser.AlterTable, *sqlparser.CreateView, *sqlparser.AlterView:
		result = append(result, NewTableSchemaAndName(schemaOfSession, ddlStmt.GetTable().Qualifier.String(), ddlStmt.GetTable().Name.String()))
	case *sqlparser.DropTable, *sqlparser.DropView:
		tables := ddlStmt.GetFromTables()
		for _, table := range tables {
			result = append(result, NewTableSchemaAndName(schemaOfSession, table.Qualifier.String(), table.Name.String()))
		}
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported statement for Online DDL: %v", sqlparser.String(ddlStmt))
	}
	return result, nil
}
