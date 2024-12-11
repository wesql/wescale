package branch

import (
	"fmt"
	"strings"
)

type SourceMySQLService struct {
	*CommonMysqlService
	mysqlService MysqlService
}

func NewSourceMySQLService(mysqlService MysqlService) *SourceMySQLService {
	return &SourceMySQLService{
		CommonMysqlService: &CommonMysqlService{
			mysqlService: mysqlService,
		},
		mysqlService: mysqlService,
	}
}

type TableInfo struct {
	database string
	name     string
}

var GetBranchSchemaBatchSize = 50

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
