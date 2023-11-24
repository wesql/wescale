/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tmutils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/schemadiff"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqlescape"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/schema"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// This file contains helper methods to deal with Schema information.

const (
	// TableBaseTable indicates the table type is a base table.
	TableBaseTable = "BASE TABLE"
	// TableView indicates the table type is a view.
	TableView = "VIEW"
)

const (
	SchemasDiff       = "schemasDiff"
	SchemasDiffDetail = "schemasDiffDetail"
	ExtraTable        = "extraTable"
	ExtraView         = "extraView"
	TableSchemaDiff   = "tableSchemaDiff"
	TableTypeDiff     = "tableTypeDiff"
)

var DiffPatterns map[string]*regexp.Regexp
var DiffFormat map[string]string

func init() {
	DiffFormat = map[string]string{
		SchemasDiff:       `schemas are different:\n%v: %v, %v: %v`,
		SchemasDiffDetail: `schemas are different:\n%v: %v\n differs from:\n%v: %v`,
		ExtraTable:        `%v has an extra table named %v`,
		ExtraView:         `%v has an extra view named %v`,
		TableSchemaDiff:   `schemas differ on table %v:\n%v: %v\n differs from:\n%v: %v`,
		TableTypeDiff:     `schemas differ on table type for table %v:\n%v: %v\n differs from:\n%v: %v`,
	}
	DiffPatterns = map[string]*regexp.Regexp{
		SchemasDiff:       regexp.MustCompile(strings.Replace(DiffFormat[SchemasDiff], "%v", "(.+)", -1)),
		SchemasDiffDetail: regexp.MustCompile(strings.Replace(DiffFormat[SchemasDiffDetail], "%v", "(.+)", -1)),
		ExtraTable:        regexp.MustCompile(strings.Replace(DiffFormat[ExtraTable], "%v", "(.+)", -1)),
		ExtraView:         regexp.MustCompile(strings.Replace(DiffFormat[ExtraView], "%v", "(.+)", -1)),
		TableSchemaDiff:   regexp.MustCompile(strings.Replace(DiffFormat[TableSchemaDiff], "%v", "(.+)", -1)),
		TableTypeDiff:     regexp.MustCompile(strings.Replace(DiffFormat[TableTypeDiff], "%v", "(.+)", -1)),
	}
}

type SchemaDiffElement struct {
	diffType string

	leftDatabase string
	left         *tabletmanagerdatapb.SchemaDefinition

	rightDatabase string
	right         *tabletmanagerdatapb.SchemaDefinition

	extraTableDatabase string
	extraTableName     string

	extraViewDatabase string
	extraViewName     string

	diffSchemaTableName string
	diffTypeTableName   string
}

type TableDiffDDL struct {
	TableName string
	DiffType  string
	Ddl       string
}

// TableDefinitionGetColumn returns the index of a column inside a
// TableDefinition.
func TableDefinitionGetColumn(td *tabletmanagerdatapb.TableDefinition, name string) (index int, ok bool) {
	lowered := strings.ToLower(name)
	for i, n := range td.Columns {
		if lowered == strings.ToLower(n) {
			return i, true
		}
	}
	return -1, false
}

// TableDefinitions is a list of TableDefinition, for sorting
type TableDefinitions []*tabletmanagerdatapb.TableDefinition

// Len returns TableDefinitions length.
func (tds TableDefinitions) Len() int {
	return len(tds)
}

// Swap used for sorting TableDefinitions.
func (tds TableDefinitions) Swap(i, j int) {
	tds[i], tds[j] = tds[j], tds[i]
}

// TableFilter is a filter for table names and types.
type TableFilter struct {
	includeViews bool

	filterTables bool
	tableNames   []string
	tableREs     []*regexp.Regexp

	filterExcludeTables bool
	excludeTableNames   []string
	excludeTableREs     []*regexp.Regexp
}

// NewTableFilter creates a TableFilter for whitelisted tables
// (tables), no denied tables (excludeTables) and optionally
// views (includeViews).
func NewTableFilter(tables, excludeTables []string, includeViews bool) (*TableFilter, error) {
	f := &TableFilter{
		includeViews: includeViews,
	}

	// Build a list of regexp to match table names against.
	// We only use regexps if the name starts and ends with '/'.
	// Otherwise the entry in the arrays is nil, and we use the original
	// table name.
	if len(tables) > 0 {
		f.filterTables = true
		for _, table := range tables {
			if strings.HasPrefix(table, "/") {
				table = strings.Trim(table, "/")
				re, err := regexp.Compile(table)
				if err != nil {
					return nil, fmt.Errorf("cannot compile regexp %v for table: %v", table, err)
				}

				f.tableREs = append(f.tableREs, re)
			} else {
				f.tableNames = append(f.tableNames, table)
			}
		}
	}

	if len(excludeTables) > 0 {
		f.filterExcludeTables = true
		for _, table := range excludeTables {
			if strings.HasPrefix(table, "/") {
				table = strings.Trim(table, "/")
				re, err := regexp.Compile(table)
				if err != nil {
					return nil, fmt.Errorf("cannot compile regexp %v for excludeTable: %v", table, err)
				}

				f.excludeTableREs = append(f.excludeTableREs, re)
			} else {
				f.excludeTableNames = append(f.excludeTableNames, table)
			}
		}
	}

	return f, nil
}

// Includes returns whether a tableName/tableType should be included in this TableFilter.
func (f *TableFilter) Includes(tableName string, tableType string) bool {
	if f.filterTables {
		matches := false
		for _, name := range f.tableNames {
			if strings.EqualFold(name, tableName) {
				matches = true
				break
			}
		}

		if !matches {
			for _, re := range f.tableREs {
				if re.MatchString(tableName) {
					matches = true
					break
				}
			}
		}

		if !matches {
			return false
		}
	}

	if f.filterExcludeTables {
		for _, name := range f.excludeTableNames {
			if strings.EqualFold(name, tableName) {
				return false
			}
		}

		for _, re := range f.excludeTableREs {
			if re.MatchString(tableName) {
				return false
			}
		}
	}

	if !f.includeViews && tableType == TableView {
		return false
	}

	return true
}

// FilterTables returns a copy which includes only whitelisted tables
// (tables), no denied tables (excludeTables) and optionally
// views (includeViews).
func FilterTables(sd *tabletmanagerdatapb.SchemaDefinition, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	copy := proto.Clone(sd).(*tabletmanagerdatapb.SchemaDefinition)
	copy.TableDefinitions = make([]*tabletmanagerdatapb.TableDefinition, 0, len(sd.TableDefinitions))

	f, err := NewTableFilter(tables, excludeTables, includeViews)
	if err != nil {
		return nil, err
	}

	for _, table := range sd.TableDefinitions {
		if f.Includes(table.Name, table.Type) {
			copy.TableDefinitions = append(copy.TableDefinitions, table)
		}
	}

	// Regenerate hash over tables because it may have changed.
	if copy.Version != "" {
		GenerateSchemaVersion(copy)
	}

	return copy, nil
}

// GenerateSchemaVersion return a unique schema version string based on
// its TableDefinitions.
func GenerateSchemaVersion(sd *tabletmanagerdatapb.SchemaDefinition) {
	hasher := md5.New()
	for _, td := range sd.TableDefinitions {
		if _, err := hasher.Write([]byte(td.Schema)); err != nil {
			panic(err) // extremely unlikely
		}
	}
	sd.Version = hex.EncodeToString(hasher.Sum(nil))
}

// SchemaDefinitionGetTable returns TableDefinition for a given table name.
func SchemaDefinitionGetTable(sd *tabletmanagerdatapb.SchemaDefinition, table string) (td *tabletmanagerdatapb.TableDefinition, ok bool) {
	for _, td := range sd.TableDefinitions {
		if td.Name == table {
			return td, true
		}
	}
	return nil, false
}

// SchemaDefinitionToSQLStrings converts a SchemaDefinition to an array of SQL strings. The array contains all
// the SQL statements needed for creating the database, tables, and views - in that order.
// All SQL statements will have {{.DatabaseName}} in place of the actual db name.
func SchemaDefinitionToSQLStrings(sd *tabletmanagerdatapb.SchemaDefinition) []string {
	sqlStrings := make([]string, 0, len(sd.TableDefinitions)+1)
	createViewSQL := make([]string, 0, len(sd.TableDefinitions))

	// Backtick database name since keyspace names appear in the routing rules, and they might need to be escaped.
	// We unescape() them first in case we have an explicitly escaped string was specified.
	createDatabaseSQL := strings.Replace(sd.DatabaseSchema, "`{{.DatabaseName}}`", "{{.DatabaseName}}", -1)
	createDatabaseSQL = strings.Replace(createDatabaseSQL, "{{.DatabaseName}}", sqlescape.EscapeID("{{.DatabaseName}}"), -1)
	sqlStrings = append(sqlStrings, createDatabaseSQL)

	for _, td := range sd.TableDefinitions {
		if schema.IsInternalOperationTableName(td.Name) {
			continue
		}
		if td.Type == TableView {
			createViewSQL = append(createViewSQL, td.Schema)
		} else {
			lines := strings.Split(td.Schema, "\n")
			for i, line := range lines {
				if strings.HasPrefix(line, "CREATE TABLE `") {
					lines[i] = strings.Replace(line, "CREATE TABLE `", "CREATE TABLE `{{.DatabaseName}}`.`", 1)
				}
			}
			sqlStrings = append(sqlStrings, strings.Join(lines, "\n"))
		}
	}

	return append(sqlStrings, createViewSQL...)
}

// DiffSchema generates a report on what's different between two SchemaDefinitions
// including views, but Vitess internal tables are ignored.
func DiffSchema(leftName string, left *tabletmanagerdatapb.SchemaDefinition, rightName string, right *tabletmanagerdatapb.SchemaDefinition, er concurrency.ErrorRecorder) []SchemaDiffElement {

	var schemaDiffs []SchemaDiffElement
	if left == nil && right == nil {
		return schemaDiffs
	}
	if left == nil || right == nil {
		er.RecordError(fmt.Errorf(DiffFormat[SchemasDiff], leftName, left, rightName, right))
		schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: SchemasDiff, left: left, leftDatabase: leftName, rightDatabase: rightName, right: right})
		return schemaDiffs
	}
	if left.DatabaseSchema != right.DatabaseSchema {
		er.RecordError(fmt.Errorf(DiffFormat[SchemasDiffDetail], leftName, left.DatabaseSchema, rightName, right.DatabaseSchema))
		schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: SchemasDiffDetail, left: left, leftDatabase: leftName, rightDatabase: rightName, right: right})

	}

	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left.TableDefinitions) && rightIndex < len(right.TableDefinitions) {
		// extra table on the left side
		if left.TableDefinitions[leftIndex].Name < right.TableDefinitions[rightIndex].Name {
			if !schema.IsInternalOperationTableName(left.TableDefinitions[leftIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[ExtraTable], leftName, left.TableDefinitions[leftIndex].Name))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraTable, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraTableDatabase: leftName, extraTableName: left.TableDefinitions[leftIndex].Name})
			}
			leftIndex++
			continue
		}

		// extra table on the right side
		if left.TableDefinitions[leftIndex].Name > right.TableDefinitions[rightIndex].Name {
			if !schema.IsInternalOperationTableName(right.TableDefinitions[rightIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[ExtraTable], rightName, right.TableDefinitions[rightIndex].Name))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraTable, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraTableDatabase: rightName, extraTableName: right.TableDefinitions[rightIndex].Name})

			}
			rightIndex++
			continue
		}

		// same name, let's see content
		if left.TableDefinitions[leftIndex].Schema != right.TableDefinitions[rightIndex].Schema {
			if !schema.IsInternalOperationTableName(left.TableDefinitions[leftIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[TableSchemaDiff], left.TableDefinitions[leftIndex].Name, leftName, left.TableDefinitions[leftIndex].Schema, rightName, right.TableDefinitions[rightIndex].Schema))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: TableSchemaDiff, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, diffSchemaTableName: left.TableDefinitions[leftIndex].Name})

			}
		}

		if left.TableDefinitions[leftIndex].Type != right.TableDefinitions[rightIndex].Type {
			if !schema.IsInternalOperationTableName(right.TableDefinitions[rightIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[TableTypeDiff], left.TableDefinitions[leftIndex].Name, leftName, left.TableDefinitions[leftIndex].Type, rightName, right.TableDefinitions[rightIndex].Type))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: TableTypeDiff, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, diffTypeTableName: left.TableDefinitions[leftIndex].Name})

			}
		}

		leftIndex++
		rightIndex++
	}

	for leftIndex < len(left.TableDefinitions) {
		if left.TableDefinitions[leftIndex].Type == TableBaseTable {
			if !schema.IsInternalOperationTableName(left.TableDefinitions[leftIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[ExtraTable], leftName, left.TableDefinitions[leftIndex].Name))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraTable, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraTableDatabase: leftName, extraTableName: left.TableDefinitions[leftIndex].Name})
			}
		}
		if left.TableDefinitions[leftIndex].Type == TableView {
			er.RecordError(fmt.Errorf(DiffFormat[ExtraView], leftName, left.TableDefinitions[leftIndex].Name))
			schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraView, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraViewDatabase: leftName, extraViewName: left.TableDefinitions[leftIndex].Name})

		}
		leftIndex++
	}
	for rightIndex < len(right.TableDefinitions) {
		if right.TableDefinitions[rightIndex].Type == TableBaseTable {
			if !schema.IsInternalOperationTableName(right.TableDefinitions[rightIndex].Name) {
				er.RecordError(fmt.Errorf(DiffFormat[ExtraTable], rightName, right.TableDefinitions[rightIndex].Name))
				schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraTable, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraTableDatabase: rightName, extraTableName: right.TableDefinitions[rightIndex].Name})

			}
		}
		if right.TableDefinitions[rightIndex].Type == TableView {
			er.RecordError(fmt.Errorf(DiffFormat[ExtraView], rightName, right.TableDefinitions[rightIndex].Name))
			schemaDiffs = append(schemaDiffs, SchemaDiffElement{diffType: ExtraView, left: left, leftDatabase: leftName, right: right, rightDatabase: rightName, extraViewDatabase: rightName, extraViewName: right.TableDefinitions[rightIndex].Name})

		}
		rightIndex++
	}
	return schemaDiffs
}

func FindTableDefinitionFromSchema(schema *tabletmanagerdatapb.SchemaDefinition, tableName string) *tabletmanagerdatapb.TableDefinition {
	for _, table := range schema.TableDefinitions {
		if table.Name == tableName {
			return table
		}
	}
	return nil
}

func AnalyzeDiffRecord(diffSchemaStrings []SchemaDiffElement, sourceDatabase, targetDatabase string, sourceSchema, targetSchema *tabletmanagerdatapb.SchemaDefinition) ([]TableDiffDDL, error) {
	var results []TableDiffDDL
	for _, schemaDiffElement := range diffSchemaStrings {
		switch schemaDiffElement.diffType {
		case SchemasDiff:
		case SchemasDiffDetail:
		case ExtraView:
			// ignore view
		case ExtraTable:
			extraDatabase := schemaDiffElement.extraTableDatabase
			extraTableName := schemaDiffElement.extraTableName
			if extraDatabase == targetDatabase {
				extraTable := FindTableDefinitionFromSchema(targetSchema, extraTableName)
				if extraTable != nil {
					results = append(results, TableDiffDDL{TableName: extraTable.Name, DiffType: ExtraTable, Ddl: extraTable.Schema})
				}
			}
		case TableSchemaDiff, TableTypeDiff:
			diffTableName := schemaDiffElement.diffSchemaTableName
			sourceTableSchema := FindTableDefinitionFromSchema(sourceSchema, diffTableName)
			targetTableSchema := FindTableDefinitionFromSchema(targetSchema, diffTableName)
			hints := &schemadiff.DiffHints{AutoIncrementStrategy: schemadiff.AutoIncrementApplyHigher}
			diff, err := schemadiff.DiffCreateTablesQueries(sourceTableSchema.Schema, targetTableSchema.Schema, hints)
			if err != nil {
				return nil, err
			}
			results = append(results, TableDiffDDL{TableName: diffTableName, DiffType: schemaDiffElement.diffType, Ddl: diff.CanonicalStatementString()})
		}
	}

	return results, nil
}

// DiffSchemaToArray diffs two schemas and return the schema diffs if there is any.
func DiffSchemaToArray(leftName string, left *tabletmanagerdatapb.SchemaDefinition, rightName string, right *tabletmanagerdatapb.SchemaDefinition) (result []string) {
	er := concurrency.AllErrorRecorder{}
	DiffSchema(leftName, left, rightName, right, &er)
	if er.HasErrors() {
		return er.ErrorStrings()
	}
	return nil
}

// SchemaChange contains all necessary information to apply a schema change.
// It should not be sent over the wire, it's just a set of parameters.
type SchemaChange struct {
	SQL              string
	Force            bool
	AllowReplication bool
	BeforeSchema     *tabletmanagerdatapb.SchemaDefinition
	AfterSchema      *tabletmanagerdatapb.SchemaDefinition
	SQLMode          string
	DbName           string
}

// Equal compares two SchemaChange objects.
func (s *SchemaChange) Equal(s2 *SchemaChange) bool {
	return s.SQL == s2.SQL &&
		s.Force == s2.Force &&
		s.AllowReplication == s2.AllowReplication &&
		proto.Equal(s.BeforeSchema, s2.BeforeSchema) &&
		proto.Equal(s.AfterSchema, s2.AfterSchema) &&
		s.DbName == s2.DbName
}
