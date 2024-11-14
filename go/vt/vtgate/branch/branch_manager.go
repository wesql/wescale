package branch

import (
	"fmt"
	"regexp"
	"strings"
)

// todo branch

const (
	BranchMetaTableQualifiedName = "mysql.branch"

	// the reason for using individual table for snapshot is to speed up branch meta table query
	BranchSnapshotTableQualifiedName = "mysql.branch_snapshot"
)

type BranchStatus string

type Branch struct {
	workflowName string
	// source info
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	// target info, will not be stored in branch meta table
	targetHost     string
	targetPort     int
	targetUser     string
	targetPassword string
	// filter rules
	include string
	exclude string
	// others
	targetDBPattern string // todo
	status          string // todo
}

// BranchWorkflowCaches map branch workflow name to branch
var BranchWorkflowCaches = make(map[string]*Branch)

func NewBranch(workflowName,
	sourceHost string, sourcePort int, sourceUser, sourcePassword,
	targetHost string, targetPort int, targetUser, targetPassword,
	include, exclude string) *Branch {
	// todo
	return &Branch{
		workflowName:   workflowName,
		sourceHost:     sourceHost,
		sourcePort:     sourcePort,
		sourceUser:     sourceUser,
		sourcePassword: sourcePassword,
		targetHost:     targetHost,
		targetPort:     targetPort,
		targetUser:     targetUser,
		targetPassword: targetPassword,
		include:        include,
		exclude:        exclude,
	}
}

func BranchCreate(workflowName,
	sourceHost string, sourcePort int, sourceUser, sourcePassword,
	targetHost string, targetPort int, targetUser, targetPassword,
	include, exclude string) error {
	branchToCreate := NewBranch(workflowName, sourceHost, sourcePort, sourceUser, sourcePassword,
		targetHost, targetPort, targetUser, targetPassword, include, exclude)

	err := branchToCreate.ensureMetaTableExists()
	if err != nil {
		return err
	}
	// If branch object with same name exists in BranchWorkflowCaches or branch meta table, return error
	if checkBranchExists(workflowName) {
		return fmt.Errorf("branch %v already exists", workflowName)
	}

	// get schema from source
	stmts, err := fetchAndFilterCreateTableStmts(sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude)

	// get databases from target
	databases, err := fetchDatabases(targetHost, targetPort, targetUser, targetPassword)
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(stmts, db)
	}

	// get SQL that creates new databases and tables in target

	//createDatabaseAndTablesSQL := getSQLCreateDatabasesAndTables(stmts)

	// apply schema to target through mysql connection.
	// ===== txn begin =====

	// snapshot

	// Store source Info and branch metadata into branch meta table

	// ===== txn commit =====
	// Create branch object in BranchWorkflowCaches

	return nil
}

func (b *Branch) BranchDiff() {
	// todo
	// SchemaDiff
	// query schemas from mysql
}

func (b *Branch) BranchPrepareMerge() {
	// todo
	// PrepareMerge
	// get schemas from source and target through mysql connection
	// calculate diffs based on merge options such as override or merge
}

func (b *Branch) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (b *Branch) BranchShow() {
	//todo
}

// #####################################################################
// from now onwards are helper functions
// todo separate common tool functions and SPI functions

// Ensure branch meta table exists in target mysql, if not exists, create it
func (b *Branch) ensureMetaTableExists() error {
	// todo
	return nil
}

func checkBranchExists(workflowName string) bool {
	// check workflowName exists in BranchWorkflowCaches
	if _, exists := BranchWorkflowCaches[workflowName]; exists {
		return true
	}

	// check from getBranchFromMetaTable  branch meta table
	if getBranchFromMetaTable(workflowName) != nil {
		return true
	}
	return false
}

func getBranchFromMetaTable(workflowName string) *Branch {
	// todo
	return nil
}

// todo spi
func fetchDatabases(host string, port int, user, password string) ([]string, error) {
	return GetAllDatabases(host, port, user, password)
}

// todo spi
func fetchAndFilterCreateTableStmts(host string, port int, user, password, include, exclude string) (map[string]map[string]string, error) {
	// Get all create table statements except system databases
	stmts, err := GetAllCreateTableStatements(host, port, user, password, []string{"mysql", "sys", "information_schema", "performance_schema"})
	if err != nil {
		return nil, err
	}
	return filterCreateTableStmts(stmts, include, exclude)
}

// return error if any pattern in `include` does not match
// if `include` is empty, return error
func filterCreateTableStmts(stmts map[string]map[string]string, include, exclude string) (map[string]map[string]string, error) {
	if include == "" {
		return nil, fmt.Errorf("include pattern is empty")
	}

	// Parse include and exclude patterns
	includePatterns := parsePatterns(include)
	excludePatterns := parsePatterns(exclude)

	// Create result map and pattern match tracking
	result := make(map[string]map[string]string)
	patternMatchCount := make(map[string]int)

	// Initialize match count for include patterns
	for _, pattern := range includePatterns {
		patternMatchCount[strings.TrimSpace(pattern)] = 0
	}

	// Process each database and table
	for dbName, tables := range stmts {
		for tableName, createStmt := range tables {
			tableId := dbName + "." + tableName

			// Check inclusion
			included := false
			for _, pattern := range includePatterns {
				pattern = strings.TrimSpace(pattern)
				if matchPattern(tableId, pattern) {
					included = true
					patternMatchCount[pattern]++
				}
			}

			if !included {
				continue
			}

			// Check exclusion
			if matchesAnyPattern(tableId, excludePatterns) {
				continue
			}

			// Add to result
			if _, exists := result[dbName]; !exists {
				result[dbName] = make(map[string]string)
			}
			result[dbName][tableName] = createStmt
		}
	}

	// Check if any include pattern had no matches
	if len(includePatterns) > 0 {
		var unmatchedPatterns []string
		for pattern, count := range patternMatchCount {
			if count == 0 {
				unmatchedPatterns = append(unmatchedPatterns, pattern)
			}
		}
		if len(unmatchedPatterns) > 0 {
			return nil, fmt.Errorf("the following include patterns had no matches: %s", strings.Join(unmatchedPatterns, ", "))
		}
	}
	return result, nil
}

// parsePatterns splits the pattern string and returns a slice of patterns
func parsePatterns(patterns string) []string {
	if patterns == "" {
		return nil
	}
	return strings.Split(patterns, ",")
}

// matchesAnyPattern checks if the tableId matches any of the patterns
func matchesAnyPattern(tableId string, patterns []string) bool {
	if patterns == nil {
		return false
	}

	for _, pattern := range patterns {
		if matchPattern(tableId, strings.TrimSpace(pattern)) {
			return true
		}
	}
	return false
}

// matchPattern checks if a table ID (db.table) matches a pattern (d.t) with wildcard support
func matchPattern(tableId, pattern string) bool {
	// Split both tableId and pattern into database and table parts
	tableParts := strings.Split(tableId, ".")
	patternParts := strings.Split(pattern, ".")

	if len(tableParts) != 2 || len(patternParts) != 2 {
		return false
	}

	// Match both database name and table name separately
	return matchWildcard(tableParts[0], patternParts[0]) &&
		matchWildcard(tableParts[1], patternParts[1])
}

// matchWildcard handles wildcard pattern matching with support for partial wildcards
func matchWildcard(s, pattern string) bool {
	// Handle plain wildcard pattern
	pattern = strings.TrimSpace(pattern)
	if pattern == "*" {
		return true
	}

	// Convert pattern to regular expression
	// 1. Escape all regex special characters
	regex := regexp.QuoteMeta(pattern)
	// 2. Replace * with .* for wildcard matching
	regex = strings.Replace(regex, "\\*", ".*", -1)
	// 3. Add start and end anchors for full string match
	regex = "^" + regex + "$"

	// Attempt to match the pattern
	matched, err := regexp.MatchString(regex, s)
	if err != nil {
		return false
	}
	return matched
}

func getSQLCreateDatabasesAndTables(createTableStmts map[string]map[string]string) string {
	finalQuery := ""
	for dbName, tables := range createTableStmts {
		temp := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;USE DATABASE %s;", dbName, dbName)
		for _, createStmt := range tables {
			temp += createStmt + ";"
		}
		finalQuery += temp
	}
	return finalQuery
}

// todo spi
func createDatabaseAndTables(host string, port int, user, password string, createTableStmts map[string]map[string]string) error {
	sqlQuery := getSQLCreateDatabasesAndTables(createTableStmts)
	if sqlQuery == "" {
		return fmt.Errorf("no SQL statements to execute")
	}
	return ExecuteSQL(host, port, user, password, sqlQuery)
}
