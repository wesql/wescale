package branch

import (
	"fmt"
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

type TableInfo struct {
	database string
	name     string
}

var GetBranchSchemaBatchSize = 50

// GetBranchSchema retrieves CREATE TABLE statements for all tables in databases filtered by `databasesInclude` and `databasesExclude`
func (s *SourceMySQLService) GetBranchSchema(databasesInclude, databasesExclude []string) (*BranchSchema, error) {
	tableInfos, err := s.getTableInfos(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}
	return s.getBranchSchemaInBatches(tableInfos, GetBranchSchemaBatchSize)
}

/**********************************************************************************************************************/

// buildTableInfosQuerySQL constructs the SQL query to retrieve table names
func buildTableInfosQuerySQL(databasesInclude, databasesExclude []string) (string, error) {
	sql := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"

	// deal with databasesInclude
	if databasesInclude == nil {
		return "", fmt.Errorf("databasesInclude is empty, which is not supported")
	}
	databasesIncludeFilterEmpty := make([]string, 0)
	for _, db := range databasesInclude {
		if db != "" {
			databasesIncludeFilterEmpty = append(databasesIncludeFilterEmpty, db)
		}
	}
	if len(databasesIncludeFilterEmpty) == 0 {
		return "", fmt.Errorf("databasesInclude is empty, which is not supported")
	}

	needInclude := true
	for _, db := range databasesIncludeFilterEmpty {
		if db == "*" {
			needInclude = false
			break
		}
	}
	if needInclude {
		includeList := strings.Join(databasesIncludeFilterEmpty, "','")
		sql += fmt.Sprintf(" AND TABLE_SCHEMA IN ('%s')", includeList)
	}

	// deal with databasesExclude
	databasesExcludeFilterEmptye := make([]string, 0)
	for _, db := range databasesExclude {
		if db == "*" {
			return "", fmt.Errorf("exclude all databases is not supported")
		}
		if db != "" {
			databasesExcludeFilterEmptye = append(databasesExcludeFilterEmptye, db)
		}
	}

	if len(databasesExcludeFilterEmptye) > 0 {
		excludeList := strings.Join(databasesExcludeFilterEmptye, "','")
		sql += fmt.Sprintf(" AND TABLE_SCHEMA NOT IN ('%s')", excludeList)
	}
	return sql, nil
}

// getTableInfos executes the table info query and returns a slice of tableInfo
func (s *SourceMySQLService) getTableInfos(databasesInclude, databasesExclude []string) ([]TableInfo, error) {
	query, err := buildTableInfosQuerySQL(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}

	rows, err := s.mysqlService.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table information: %v", err)
	}
	defer rows.Close()

	var tableInfos []TableInfo

	for rows.Next() {
		var database, tableName string
		if err := rows.Scan(&database, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan query result: %v", err)
		}
		tableInfos = append(tableInfos, TableInfo{database: database, name: tableName})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating query results: %v", err)
	}

	return tableInfos, nil
}

// getBranchSchemaInBatches retrieves CREATE TABLE statements in batches
func (s *SourceMySQLService) getBranchSchemaInBatches(tableInfos []TableInfo, batchSize int) (*BranchSchema, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < len(tableInfos); i += batchSize {
		end := i + batchSize
		if end > len(tableInfos) {
			end = len(tableInfos)
		}
		batch := tableInfos[i:end]

		combinedQuery := getCombinedShowCreateTableSQL(batch)

		// Execute the combined query
		multiRows, err := s.mysqlService.Query(combinedQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to execute combined query: %v", err)
		}

		// Process each result set in the batch
		for j := 0; j < len(batch); j++ {
			table := batch[j]
			db := table.database
			tableName := table.name

			// Ensure database map is initialized
			if _, exists := result[db]; !exists {
				result[db] = make(map[string]string)
			}

			// Each SHOW CREATE TABLE result has two columns: Table and Create Table
			if !multiRows.Next() {
				return nil, fmt.Errorf("unexpected end of result sets while processing %s.%s", db, tableName)
			}

			var tableNameResult, createTableStmt string
			if err := multiRows.Scan(&tableNameResult, &createTableStmt); err != nil {
				return nil, fmt.Errorf("failed to scan create table result for %s.%s: %v", db, tableName, err)
			}

			// Store the result
			result[db][tableName] = createTableStmt

			// Move to next result set, unless it's the last table in the batch
			if j < len(batch)-1 {
				if !multiRows.NextResultSet() {
					return nil, fmt.Errorf("failed to move to next result set after processing %s.%s", db, tableName)
				}
			}
		}

		multiRows.Close()
	}

	return &BranchSchema{schema: result}, nil
}

func getCombinedShowCreateTableSQL(tableInfos []TableInfo) string {
	// Build combined query for the batch
	var showStatements []string
	for _, table := range tableInfos {
		showStmt := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", table.database, table.name)
		showStatements = append(showStatements, showStmt)
	}
	combinedQuery := strings.Join(showStatements, " ")
	return combinedQuery
}
