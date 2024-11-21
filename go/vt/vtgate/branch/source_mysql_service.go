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

var BranchSchemaInBatches = 50

// GetBranchSchema retrieves CREATE TABLE statements for all tables in all databases
// Returns a nested map where the first level key is the database name,
// second level key is the table name, and the value is the CREATE TABLE statement
func (s *SourceMySQLService) GetBranchSchema(databasesExclude []string) (*BranchSchema, error) {
	tableInfos, err := s.getTableInfos(databasesExclude)
	if err != nil {
		return nil, err
	}
	return s.getBranchSchemaInBatches(tableInfos, BranchSchemaInBatches)
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

// buildTableInfosQuerySQL constructs the SQL query to retrieve table names
func buildTableInfosQuerySQL(databasesExclude []string) string {
	sql := "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"

	if databasesExclude != nil && len(databasesExclude) > 0 {
		excludeList := strings.Join(databasesExclude, "','")
		sql += fmt.Sprintf(" AND TABLE_SCHEMA NOT IN ('%s')", excludeList)
	}

	return sql
}

// getTableInfos executes the table info query and returns a slice of tableInfo
func (s *SourceMySQLService) getTableInfos(databasesExclude []string) ([]TableInfo, error) {

	query := buildTableInfosQuerySQL(databasesExclude)

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
