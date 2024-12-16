package branch

import "fmt"

type CommonMysqlService struct {
	mysqlService MysqlService
}

// GetBranchSchema retrieves CREATE TABLE statements for all tables in databases filtered by `databasesInclude` and `databasesExclude`
func (c *CommonMysqlService) GetBranchSchema(databasesInclude, databasesExclude []string) (*BranchSchema, error) {
	tableInfos, err := c.getTableInfos(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}
	if len(tableInfos) == 0 {
		return nil, fmt.Errorf("no table found")
	}

	return c.getTableSchemaOneByOne(tableInfos)
}

/**********************************************************************************************************************/

// getTableInfos executes the table info query and returns a slice of tableInfo
func (c *CommonMysqlService) getTableInfos(databasesInclude, databasesExclude []string) ([]TableInfo, error) {
	var tableInfos []TableInfo

	lastSchema := ""
	lastTable := ""

	for {
		query, err := buildTableInfosQueryInBatchSQL(databasesInclude, databasesExclude, lastSchema, lastTable, SelectBatchSize)
		if err != nil {
			return nil, err
		}

		rows, err := c.mysqlService.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to query table information: %v", err)
		}

		for _, row := range rows {
			var database, tableName string
			database = BytesToString(row.RowData["TABLE_SCHEMA"])
			tableName = BytesToString(row.RowData["TABLE_NAME"])

			tableInfos = append(tableInfos, TableInfo{database: database, name: tableName})
		}

		if len(rows) < SelectBatchSize {
			break
		}

		lastSchema = BytesToString(rows[SelectBatchSize-1].RowData["TABLE_SCHEMA"])
		lastTable = BytesToString(rows[SelectBatchSize-1].RowData["TABLE_NAME"])

	}

	return tableInfos, nil
}

// get table schema one by one
func (c *CommonMysqlService) getTableSchemaOneByOne(tableInfos []TableInfo) (*BranchSchema, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < len(tableInfos); i++ {

		query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", tableInfos[i].database, tableInfos[i].name)

		rows, err := c.mysqlService.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query %v: %v", query, err)
		}

		for _, row := range rows {
			if _, exists := result[tableInfos[i].database]; !exists {
				result[tableInfos[i].database] = make(map[string]string)
			}
			result[tableInfos[i].database][tableInfos[i].name] = BytesToString(row.RowData["Create Table"])
		}
	}

	return &BranchSchema{branchSchema: result}, nil
}
