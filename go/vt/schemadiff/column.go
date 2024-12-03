/*
Copyright 2022 The Vitess Authors.

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

package schemadiff

import (
	"fmt"
	"strings"
	"vitess.io/vitess/go/vt/sqlparser"
)

// columnDetails decorates a column with more details, used by diffing logic
type columnDetails struct {
	col     *sqlparser.ColumnDefinition
	prevCol *columnDetails // previous in sequence in table definition
	nextCol *columnDetails // next in sequence in table definition
}

func (c *columnDetails) identicalOtherThanName(other *sqlparser.ColumnDefinition) bool {
	if other == nil {
		return false
	}
	return sqlparser.Equals.SQLNode(c.col.Type, other.Type)
}

func (c *columnDetails) prevColName() string {
	if c.prevCol == nil {
		return ""
	}
	return c.prevCol.col.Name.String()
}

func (c *columnDetails) nextColName() string {
	if c.nextCol == nil {
		return ""
	}
	return c.nextCol.col.Name.String()
}

func getColName(id *sqlparser.IdentifierCI) *sqlparser.ColName {
	return &sqlparser.ColName{Name: *id}
}

type ModifyColumnDiff struct {
	modifyColumn *sqlparser.ModifyColumn
}

func NewModifyColumnDiff(modifyColumn *sqlparser.ModifyColumn) *ModifyColumnDiff {
	return &ModifyColumnDiff{modifyColumn: modifyColumn}
}

func NewModifyColumnDiffByDefinition(definition *sqlparser.ColumnDefinition) *ModifyColumnDiff {
	modifyColumn := &sqlparser.ModifyColumn{
		NewColDefinition: definition,
	}
	return NewModifyColumnDiff(modifyColumn)
}

type ColumnDefinitionEntity struct {
	ColumnDefinition    *sqlparser.ColumnDefinition
	tableCharsetCollate *charsetCollate
}

func NewColumnDefinitionEntity(c *sqlparser.ColumnDefinition, tableCharsetCollate *charsetCollate) *ColumnDefinitionEntity {
	return &ColumnDefinitionEntity{ColumnDefinition: c, tableCharsetCollate: tableCharsetCollate}
}

// ColumnDiff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an AlterTable statement if changes are found, or nil if not.
// the other table may be of different name; its name is ignored.
func (c *ColumnDefinitionEntity) ColumnDiff(other *ColumnDefinitionEntity, hints *DiffHints) (*ModifyColumnDiff, error) {
	cClone := c         // not real clone yet
	otherClone := other // not real clone yet

	if c.IsTextual() || other.IsTextual() {
		cClone = c.Clone()
		otherClone = other.Clone()
		switch hints.ColumnCharsetCollateStrategy {
		case ColumnCharsetCollateStrict:
			if err := cClone.SetExplicitCharsetCollate(); err != nil {
				return nil, err
			}

			if err := otherClone.SetExplicitCharsetCollate(); err != nil {
				return nil, err
			}

		case ColumnCharsetCollateIgnoreAlways:
			cClone.SetCharsetCollateEmpty()
			otherClone.SetCharsetCollateEmpty()
		}
	}
	if sqlparser.Equals.RefOfColumnDefinition(c.ColumnDefinition, other.ColumnDefinition) {
		return nil, nil
	}

	return NewModifyColumnDiffByDefinition(otherClone.ColumnDefinition), nil
}

// IsTextual returns true when this column is of textual type, and is capable of having a character set property
func (c *ColumnDefinitionEntity) IsTextual() bool {
	return charsetTypes[strings.ToLower(c.ColumnDefinition.Type.Type)]
}

func (c *ColumnDefinitionEntity) Clone() *ColumnDefinitionEntity {
	clone := &ColumnDefinitionEntity{
		ColumnDefinition:    sqlparser.Clone(c.ColumnDefinition),
		tableCharsetCollate: c.tableCharsetCollate,
	}
	return clone
}

// SetExplicitCharsetCollate enriches this column definition with collation and charset. Those may be
// already present, or perhaps just one of them is present (in which case we use the one to populate the other),
// or both might be missing, in which case we use the table's charset/collation.
// Normally in schemadiff we work the opposite way: we strive to have the minimal equivalent representation
// of a definition. But this function can be used (often in conjunction with Clone()) to enrich a column definition
// so as to have explicit and authoritative view on any particular column.
func (c *ColumnDefinitionEntity) SetExplicitCharsetCollate() error {
	if !c.IsTextual() {
		return nil
	}
	// We will now denormalize the columns charset & collate as needed (if empty, populate from table.)
	// Normalizing _this_ column definition:
	if c.ColumnDefinition.Type.Charset.Name != "" && c.ColumnDefinition.Type.Options.Collate == "" {
		// Charset defined without collation. Assign the default collation for that charset.
		collation := defaultCharsetForCollation(c.ColumnDefinition.Type.Charset.Name)
		if collation == "" {
			if collation == "" {
				return fmt.Errorf("unable to determine collation for column %s with charset %s", c.ColumnDefinition.Name, c.tableCharsetCollate.charset)
			}
		}
		c.ColumnDefinition.Type.Options.Collate = collation
	}
	if c.ColumnDefinition.Type.Charset.Name == "" && c.ColumnDefinition.Type.Options.Collate != "" {
		// Column has explicit collation but no charset. We can infer the charset from the collation.
		collationID := collationEnv.LookupByName(c.ColumnDefinition.Type.Options.Collate)
		charset := collationEnv.LookupCharsetName(collationID.ID())
		if charset == "" {
			return fmt.Errorf("unable to determine charset for column %s with collation %s", c.ColumnDefinition.Name, collationID.Name())
		}
		c.ColumnDefinition.Type.Charset.Name = charset
	}
	if c.ColumnDefinition.Type.Charset.Name == "" {
		// Still nothing? Assign the table's charset/collation.
		c.ColumnDefinition.Type.Charset.Name = c.tableCharsetCollate.charset
		if c.ColumnDefinition.Type.Options.Collate == "" {
			c.ColumnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate
		}
		if c.ColumnDefinition.Type.Options.Collate = c.tableCharsetCollate.collate; c.ColumnDefinition.Type.Options.Collate == "" {

			collation := defaultCharsetForCollation(c.tableCharsetCollate.charset)
			if collation == "" {
				return fmt.Errorf("unable to determine collation for column %s with charset %s", c.ColumnDefinition.Name, c.tableCharsetCollate.charset)
			}

			c.ColumnDefinition.Type.Options.Collate = collation
		}
	}
	return nil
}

func (c *ColumnDefinitionEntity) SetCharsetCollateEmpty() {
	if c.IsTextual() {
		c.ColumnDefinition.Type.Charset.Name = ""
		c.ColumnDefinition.Type.Charset.Binary = false
		c.ColumnDefinition.Type.Options.Collate = ""
	}
}
