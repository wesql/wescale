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

	return c.getBranchSchema(tableInfos)
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

	var tableInfos []TableInfo

	for _, row := range rows {
		var database, tableName string
		database = BytesToString(row.RowData["TABLE_SCHEMA"])
		tableName = BytesToString(row.RowData["TABLE_NAME"])

		tableInfos = append(tableInfos, TableInfo{database: database, name: tableName})
	}

	return tableInfos, nil
}

// get table schema one by one
func (c *CommonMysqlService) getBranchSchema(tableInfos []TableInfo) (*BranchSchema, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < len(tableInfos); i++ {

		query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", tableInfos[i].database, tableInfos[i].name)

		rows, err := c.mysqlService.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query %v: %v", query, err)
		}

		for _, row := range rows {
			result[tableInfos[i].database] = make(map[string]string)
			result[tableInfos[i].database][tableInfos[i].name] = BytesToString(row.RowData["Create Table"])
		}
	}

	return &BranchSchema{branchSchema: result}, nil
}
