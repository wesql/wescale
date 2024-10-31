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
	"errors"
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Entity stands for a database object we can diff:
// - A table
// - A view
type Entity interface {
	// Name of entity, ie table name, view name, etc.
	Name() string
	// Diff returns an entitty diff given another entity. The diff direction is from this entity and to the other entity.
	Diff(other Entity, hints *DiffHints) (diff EntityDiff, err error)
	// Create returns an entity diff that describes how to create this entity
	Create() EntityDiff
	// Drop returns an entity diff that describes how to drop this entity
	Drop() EntityDiff
	// Clone returns a deep copy of the entity.
	Clone() Entity
}

// EntityDiff represents the diff between two entities
type EntityDiff interface {
	// IsEmpty returns true when the two entities are considered identical
	IsEmpty() bool
	// Entities returns the two diffed entitied, aka "from" and "to"
	Entities() (from Entity, to Entity)
	// Statement returns a valid SQL statement that applies the diff, e.g. an ALTER TABLE ...
	// It returns nil if the diff is empty
	Statement() sqlparser.Statement
	// StatementString "stringifies" this diff's Statement(). It returns an empty string if the diff is empty
	StatementString() string
	// CanonicalStatementString "stringifies" this diff's Statement() to a canonical string. It returns an empty string if the diff is empty
	CanonicalStatementString() string
	// SubsequentDiff returns a followup diff to this one, if exists
	SubsequentDiff() EntityDiff
	// SetSubsequentDiff updates the existing subsequent diff to the given one
	SetSubsequentDiff(EntityDiff)
}

const (
	AutoIncrementIgnore int = iota
	AutoIncrementApplyHigher
	AutoIncrementApplyAlways
)

var AutoIncrementStrategyValues = map[int]string{
	AutoIncrementIgnore:      "ignore",
	AutoIncrementApplyHigher: "apply_higher",
	AutoIncrementApplyAlways: "apply_always",
}

var autoIncrementStrategyStrings = map[string]int{
	AutoIncrementStrategyValues[AutoIncrementIgnore]:      AutoIncrementIgnore,
	AutoIncrementStrategyValues[AutoIncrementApplyHigher]: AutoIncrementApplyHigher,
	AutoIncrementStrategyValues[AutoIncrementApplyAlways]: AutoIncrementApplyAlways,
}

func ParseAutoIncrementStrategy(strategyStr string) (int, error) {
	if strategy, exists := autoIncrementStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid auto increment strategy: " + strategyStr)
}

func AutoIncrementStrategyToString(strategy int) (string, error) {
	if str, exists := AutoIncrementStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid auto increment strategy value: " + fmt.Sprint(strategy))
}

const (
	RangeRotationFullSpec = iota
	RangeRotationDistinctStatements
	RangeRotationIgnore
)

var RangeRotationStrategyValues = map[int]string{
	RangeRotationFullSpec:           "full_spec",
	RangeRotationDistinctStatements: "distinct_statements",
	RangeRotationIgnore:             "ignore",
}

var rangeRotationStrategyStrings = map[string]int{
	RangeRotationStrategyValues[RangeRotationFullSpec]:           RangeRotationFullSpec,
	RangeRotationStrategyValues[RangeRotationDistinctStatements]: RangeRotationDistinctStatements,
	RangeRotationStrategyValues[RangeRotationIgnore]:             RangeRotationIgnore,
}

func ParseRangeRotationStrategy(strategyStr string) (int, error) {
	if strategy, exists := rangeRotationStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid range rotation strategy: " + strategyStr)
}

func RangeRotationStrategyToString(strategy int) (string, error) {
	if str, exists := RangeRotationStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid range rotation strategy value: " + fmt.Sprint(strategy))
}

const (
	ConstraintNamesIgnoreVitess = iota
	ConstraintNamesIgnoreAll
	ConstraintNamesStrict
)

var ConstraintNamesStrategyValues = map[int]string{
	ConstraintNamesIgnoreVitess: "ignore_vitess",
	ConstraintNamesIgnoreAll:    "ignore_all",
	ConstraintNamesStrict:       "strict",
}

var constraintNamesStrategyStrings = map[string]int{
	ConstraintNamesStrategyValues[ConstraintNamesIgnoreVitess]: ConstraintNamesIgnoreVitess,
	ConstraintNamesStrategyValues[ConstraintNamesIgnoreAll]:    ConstraintNamesIgnoreAll,
	ConstraintNamesStrategyValues[ConstraintNamesStrict]:       ConstraintNamesStrict,
}

func ParseConstraintNamesStrategy(strategyStr string) (int, error) {
	if strategy, exists := constraintNamesStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid constraint names strategy: " + strategyStr)
}

func ConstraintNamesStrategyToString(strategy int) (string, error) {
	if str, exists := ConstraintNamesStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid constraint names strategy value: " + fmt.Sprint(strategy))
}

const (
	ColumnRenameAssumeDifferent = iota
	ColumnRenameHeuristicStatement
)

var ColumnRenameStrategyValues = map[int]string{
	ColumnRenameAssumeDifferent:    "assume_different",
	ColumnRenameHeuristicStatement: "heuristic_statement",
}

var columnRenameStrategyStrings = map[string]int{
	ColumnRenameStrategyValues[ColumnRenameAssumeDifferent]:    ColumnRenameAssumeDifferent,
	ColumnRenameStrategyValues[ColumnRenameHeuristicStatement]: ColumnRenameHeuristicStatement,
}

func ParseColumnRenameStrategy(strategyStr string) (int, error) {
	if strategy, exists := columnRenameStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid column rename strategy: " + strategyStr)
}

func ColumnRenameStrategyToString(strategy int) (string, error) {
	if str, exists := ColumnRenameStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid column rename strategy value: " + fmt.Sprint(strategy))
}

const (
	TableRenameAssumeDifferent = iota
	TableRenameHeuristicStatement
)

var TableRenameStrategyValues = map[int]string{
	TableRenameAssumeDifferent:    "assume_different",
	TableRenameHeuristicStatement: "heuristic_statement",
}

var tableRenameStrategyStrings = map[string]int{
	TableRenameStrategyValues[TableRenameAssumeDifferent]:    TableRenameAssumeDifferent,
	TableRenameStrategyValues[TableRenameHeuristicStatement]: TableRenameHeuristicStatement,
}

func ParseTableRenameStrategy(strategyStr string) (int, error) {
	if strategy, exists := tableRenameStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid table rename strategy: " + strategyStr)
}

func TableRenameStrategyToString(strategy int) (string, error) {
	if str, exists := TableRenameStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid table rename strategy value: " + fmt.Sprint(strategy))
}

const (
	FullTextKeyDistinctStatements = iota
	FullTextKeyUnifyStatements
)

var FullTextKeyStrategyValues = map[int]string{
	FullTextKeyDistinctStatements: "distinct_statements",
	FullTextKeyUnifyStatements:    "unify_statements",
}

var fullTextKeyStrategyStrings = map[string]int{
	FullTextKeyStrategyValues[FullTextKeyDistinctStatements]: FullTextKeyDistinctStatements,
	FullTextKeyStrategyValues[FullTextKeyUnifyStatements]:    FullTextKeyUnifyStatements,
}

func ParseFullTextKeyStrategy(strategyStr string) (int, error) {
	if strategy, exists := fullTextKeyStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid full text key strategy: " + strategyStr)
}

func FullTextKeyStrategyToString(strategy int) (string, error) {
	if str, exists := FullTextKeyStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid full text key strategy value: " + fmt.Sprint(strategy))
}

const (
	TableCharsetCollateStrict int = iota
	TableCharsetCollateIgnoreEmpty
	TableCharsetCollateIgnoreAlways
)

var TableCharsetCollateStrategyValues = map[int]string{
	TableCharsetCollateStrict:       "strict",
	TableCharsetCollateIgnoreEmpty:  "ignore_empty",
	TableCharsetCollateIgnoreAlways: "ignore_always",
}

var tableCharsetCollateStrategyStrings = map[string]int{
	TableCharsetCollateStrategyValues[TableCharsetCollateStrict]:       TableCharsetCollateStrict,
	TableCharsetCollateStrategyValues[TableCharsetCollateIgnoreEmpty]:  TableCharsetCollateIgnoreEmpty,
	TableCharsetCollateStrategyValues[TableCharsetCollateIgnoreAlways]: TableCharsetCollateIgnoreAlways,
}

func ParseTableCharsetCollateStrategy(strategyStr string) (int, error) {
	if strategy, exists := tableCharsetCollateStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid table charset collate strategy: " + strategyStr)
}

func TableCharsetCollateStrategyToString(strategy int) (string, error) {
	if str, exists := TableCharsetCollateStrategyValues[strategy]; exists {
		return str, nil
	}
	return "", errors.New("invalid table charset collate strategy value: " + fmt.Sprint(strategy))
}

const (
	AlterTableAlgorithmStrategyNone int = iota
	AlterTableAlgorithmStrategyInstant
	AlterTableAlgorithmStrategyInplace
	AlterTableAlgorithmStrategyCopy
)

var AlterTableAlgorithmStrategyValues = map[int]string{
	AlterTableAlgorithmStrategyNone:    "none",
	AlterTableAlgorithmStrategyInstant: "instant",
	AlterTableAlgorithmStrategyInplace: "inplace",
	AlterTableAlgorithmStrategyCopy:    "copy",
}

var alterTableAlgorithmStrategyStrings = map[string]int{
	AlterTableAlgorithmStrategyValues[AlterTableAlgorithmStrategyNone]:    AlterTableAlgorithmStrategyNone,
	AlterTableAlgorithmStrategyValues[AlterTableAlgorithmStrategyInstant]: AlterTableAlgorithmStrategyInstant,
	AlterTableAlgorithmStrategyValues[AlterTableAlgorithmStrategyInplace]: AlterTableAlgorithmStrategyInplace,
	AlterTableAlgorithmStrategyValues[AlterTableAlgorithmStrategyCopy]:    AlterTableAlgorithmStrategyCopy,
}

func ParseAlterTableAlgorithmStrategy(strategyStr string) (int, error) {
	if strategy, exists := alterTableAlgorithmStrategyStrings[strategyStr]; exists {
		return strategy, nil
	}
	return -1, errors.New("invalid alter table algorithm strategy: " + strategyStr)
}

// DiffHints is an assortment of rules for diffing entities
type DiffHints struct {
	StrictIndexOrdering         bool
	AutoIncrementStrategy       int
	RangeRotationStrategy       int
	ConstraintNamesStrategy     int
	ColumnRenameStrategy        int
	TableRenameStrategy         int
	FullTextKeyStrategy         int
	TableCharsetCollateStrategy int
	AlterTableAlgorithmStrategy int
}
