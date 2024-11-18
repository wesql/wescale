package branch

import "fmt"

// todo: do not use mysqlService.db
type TargetMySQLService struct {
	mysqlService *MysqlService
}

func (t *TargetMySQLService) ensureMetaTableExists() error {
	err := t.mysqlService.ExecuteSQL(CreateBranchMetaTableSQL)
	if err != nil {
		return err
	}
	return t.mysqlService.ExecuteSQL(CreateBranchSnapshotTableSQL)
}

func (t *TargetMySQLService) FetchDatabases() ([]string, error) {
	//todo
	return nil, nil
}

func (t *TargetMySQLService) CreateNewDatabaseAndTables(createTableStmts map[string]map[string]string) error {
	sqlQuery := getSQLCreateDatabasesAndTables(createTableStmts)
	if sqlQuery == "" {
		return fmt.Errorf("no SQL statements to execute")
	}
	return t.mysqlService.ExecuteSQL(sqlQuery)
}

func (t *TargetMySQLService) getBranchFromMetaTable(name string) *BranchService {
	// todo
	return nil
}

// todo branch: snapshot and branchMeta should have their struct type
func (t *TargetMySQLService) storeMetaData(snapshot map[string]map[string]string, branchMeta BranchMeta) error {
	// todo
	//insertSnapshotSQL := getInsertSnapshotSQL(branchMeta.name, string(snapshot))
	//insertBranchMetaSQL := getInsertBranchMetaSQL(branchMeta.name, sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude, "create", "")

	// todo: need to call: checkBranchExists

	return nil
}

func (t *TargetMySQLService) checkBranchExists(name string) bool {
	//todo
	return false
}

func getInsertSnapshotSQL(name, snapshotData string) string {
	return fmt.Sprintf(InsertBranchSnapshotSQL, name, snapshotData)
}

func getInsertBranchMetaSQL(name, sourceHost string, sourcePort int, sourceUser, sourcePassword, include, exclude, status, targetDBPattern string) string {
	return fmt.Sprintf(InsertBranchSQL,
		name,
		sourceHost,
		sourcePort,
		sourceUser,
		sourcePassword,
		include,
		exclude,
		status,
		targetDBPattern)
}
