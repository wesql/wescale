package branch

import (
	"fmt"
	"regexp"
	"strings"
)

type SourceMySQLService struct {
	mysqlService *MysqlService
}

func NewSourceMySQLService(mysqlService *MysqlService) *SourceMySQLService {
	return &SourceMySQLService{
		mysqlService: mysqlService,
	}
}

// todo branch add UT
// GetAllCreateTableStatements retrieves CREATE TABLE statements for all tables in all databases
// Returns a nested map where the first level key is the database name,
// second level key is the table name, and the value is the CREATE TABLE statement
func (s *SourceMySQLService) GetAllCreateTableStatements(databasesExclude []string) (map[string]map[string]string, error) {
	// todo: need to paginate the query, because the result set may be too large. maybe add a param to control it.

	// todo: split this method into smaller methods for better readability and testability

	// First step: Get information about all tables and build the combined query
	buildQuery := `
        SELECT CONCAT( 'SHOW CREATE TABLE ', TABLE_SCHEMA, '.', TABLE_NAME, ';' ) AS show_stmt,
               TABLE_SCHEMA,
               TABLE_NAME
        FROM information_schema.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        `

	if databasesExclude != nil && len(databasesExclude) > 0 {
		buildQuery += fmt.Sprintf(" AND TABLE_SCHEMA NOT IN ('%s')", strings.Join(databasesExclude, "','"))
	}

	rows, err := s.mysqlService.Query(buildQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query table information: %v", err)
	}
	defer rows.Close()

	// Collect all statements and table information
	var showStatements []string
	type tableInfo struct {
		schema string
		name   string
	}
	tableInfos := make([]tableInfo, 0)

	for rows.Next() {
		var showStmt, schema, tableName string
		if err := rows.Scan(&showStmt, &schema, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan query result: %v", err)
		}
		showStatements = append(showStatements, showStmt)
		tableInfos = append(tableInfos, tableInfo{schema: schema, name: tableName})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating query results: %v", err)
	}

	// Build the combined query
	combinedQuery := strings.Join(showStatements, "")

	// Execute the combined query to get all CREATE TABLE statements at once
	multiRows, err := s.mysqlService.Query(combinedQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute combined query: %v", err)
	}
	defer multiRows.Close()

	// Initialize result map
	result := make(map[string]map[string]string)

	// Process each result set
	for i := 0; i < len(tableInfos); i++ {
		schema := tableInfos[i].schema
		tableName := tableInfos[i].name

		// Ensure database map is initialized
		if _, exists := result[schema]; !exists {
			result[schema] = make(map[string]string)
		}

		// Each SHOW CREATE TABLE result has two columns: table name and create statement
		if !multiRows.Next() {
			return nil, fmt.Errorf("unexpected end of result sets while processing %s.%s", schema, tableName)
		}

		var tableNameResult, createTableStmt string
		if err := multiRows.Scan(&tableNameResult, &createTableStmt); err != nil {
			return nil, fmt.Errorf("failed to scan create table result for %s.%s: %v", schema, tableName, err)
		}

		// Store the result
		result[schema][tableName] = createTableStmt

		// Move to next result set
		if i < len(tableInfos)-1 {
			if !multiRows.NextResultSet() {
				return nil, fmt.Errorf("failed to move to next result set after processing %s.%s", schema, tableName)
			}
		}
	}

	return result, nil
}

// todo not urgent: parama "mysql", "sys", "information_schema", "performance_schema" from branch_service
func (s *SourceMySQLService) FetchAndFilterCreateTableStmts(include, exclude string) (map[string]map[string]string, error) {
	// Get all create table statements except system databases
	stmts, err := s.GetAllCreateTableStatements([]string{"mysql", "sys", "information_schema", "performance_schema"})
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
