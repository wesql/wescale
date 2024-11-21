package branch

import (
	"fmt"
	"regexp"
	"strings"
)

var DefaultDatabasesToSkip = []string{"mysql", "sys", "information_schema", "performance_schema"}

type BranchService struct {
	sourceMySQLService *SourceMySQLService
	targetMySQLService *TargetMySQLService
}

func NewBranchService(sourceHandler *SourceMySQLService, targetHandler *TargetMySQLService) *BranchService {
	return &BranchService{
		sourceMySQLService: sourceHandler,
		targetMySQLService: targetHandler,
	}
}

// todo, think of failure handling
func (s *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	// get schema from source and store to target
	branchSchema, err := s.BranchFetch(branchMeta)
	if err != nil {
		return err
	}

	// stmts act as the WAL for CreateDatabaseAndTablesIfNotExists
	err = s.targetMySQLService.CreateDatabaseAndTablesIfNotExists(branchSchema)
	if err != nil {
		return err
	}

	// todo, wait for tables created

	return nil
}

func (s *BranchService) BranchFetch(branchMeta *BranchMeta) (*BranchSchema, error) {
	// Get all create table statements except system databases
	schema, err := s.sourceMySQLService.GetBranchSchema(DefaultDatabasesToSkip)
	if err != nil {
		return nil, err
	}
	err = filterBranchSchema(schema, branchMeta.include, branchMeta.exclude)
	if err != nil {
		return nil, err
	}
	err = s.targetMySQLService.StoreBranchMeta(schema, branchMeta) // this step is the commit point of BranchCreate function
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (b *BranchService) BranchDiff() {
	// todo
	// SchemaDiff
	// query schemas from mysql
}

func (b *BranchService) BranchPrepareMerge(meta BranchMeta) {
	// todo
	// PrepareMerge
	// get schemas from source and target through mysql connection
	// calculate diffs based on merge options such as override or merge
}

func (b *BranchService) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (b *BranchService) BranchShow() {
	//todo
}

/**********************************************************************************************************************/

func filterBranchSchema(schema *BranchSchema, include, exclude string) error {
	if include == "" {
		return fmt.Errorf("include pattern is empty")
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
	for dbName, tables := range schema.schema {
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

			if !included || matchesAnyPattern(tableId, excludePatterns) {
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
			return fmt.Errorf("the following include patterns had no matches: %s", strings.Join(unmatchedPatterns, ", "))
		}
	}
	schema.schema = result
	return nil
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
