package branch

import "fmt"

type CommonMysqlService struct {
	mysqlService *MysqlService
}

// GetBranchSchema retrieves CREATE TABLE statements for all tables in databases filtered by `databasesInclude` and `databasesExclude`
func (c *CommonMysqlService) GetBranchSchema(databasesInclude, databasesExclude []string) (*BranchSchema, error) {
	tableInfos, err := c.getTableInfos(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}
	return c.getBranchSchemaInBatches(tableInfos, GetBranchSchemaBatchSize)
}

/**********************************************************************************************************************/

// getTableInfos executes the table info query and returns a slice of tableInfo
func (c *CommonMysqlService) getTableInfos(databasesInclude, databasesExclude []string) ([]TableInfo, error) {
	query, err := buildTableInfosQuerySQL(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}

	rows, err := c.mysqlService.Query(query)
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
func (c *CommonMysqlService) getBranchSchemaInBatches(tableInfos []TableInfo, batchSize int) (*BranchSchema, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < len(tableInfos); i += batchSize {
		end := i + batchSize
		if end > len(tableInfos) {
			end = len(tableInfos)
		}
		batch := tableInfos[i:end]

		combinedQuery := getCombinedShowCreateTableSQL(batch)

		// Execute the combined query
		multiRows, err := c.mysqlService.Query(combinedQuery)
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

	return &BranchSchema{branchSchema: result}, nil
}
