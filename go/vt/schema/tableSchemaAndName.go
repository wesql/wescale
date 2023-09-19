/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import "fmt"

type TableSchemaAndName struct {
	tableSchema string
	tableName   string
}

// NewTableSchemaAndName creates a new TableSchemaAndName object.
// schemaOfSession is the schema of the session, which is used as the default schema if tableSchema is empty.
// tableSchema is the schema of the table, which is optional.
// tableName is the name of the table.
func NewTableSchemaAndName(schemaOfSession, tableSchema, tableName string) TableSchemaAndName {
	fullTableName := TableSchemaAndName{
		tableSchema: tableSchema,
		tableName:   tableName,
	}
	if tableSchema == "" {
		fullTableName.tableSchema = schemaOfSession
	}
	return fullTableName
}

func (f TableSchemaAndName) String() string {
	return fmt.Sprintf("%s.%s", f.tableSchema, f.tableName)
}

func (f TableSchemaAndName) IsEmpty() bool {
	return f.tableSchema == "" && f.tableName == ""
}

func (f TableSchemaAndName) GetTableSchema() string {
	return f.tableSchema
}

func (f TableSchemaAndName) GetTableName() string {
	return f.tableName
}
