package branch

import "fmt"

// todo: do not use mysqlService.db
type TargetMySQLService struct {
	mysqlService *MysqlService
}

func (t *TargetMySQLService) CreateDatabaseAndTablesIfNotExists(createTableStmts map[string]map[string]string) error {
	// get databases from target
	databases, err := t.getAllDatabases()
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(createTableStmts, db)
	}

	// apply schema to target
	err = t.CreateDatabaseAndTablesIfNotExists(createTableStmts)
	if err != nil {
		return err
	}
	return nil
}

/**********************************************************************************************************************/

// todo delete this
func (t *TargetMySQLService) ensureMetaTableExists() error {
	err := t.mysqlService.ExecuteInTxn(CreateBranchMetaTableSQL)
	if err != nil {
		return err
	}
	return t.mysqlService.ExecuteInTxn(CreateBranchSnapshotTableSQL)
}

// todo branch add UT
// getAllDatabases retrieves all database names from MySQL
func (s *TargetMySQLService) getAllDatabases() ([]string, error) {
	// Execute query to get all database names
	rows, err := s.mysqlService.Query("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("failed to query database list: %v", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %v", err)
		}
		databases = append(databases, dbName)
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating database list: %v", err)
	}

	return databases, nil
}

func (t *TargetMySQLService) createDatabaseAndTables(createTableStmts map[string]map[string]string) error {
	sqlQuery := getSQLCreateDatabasesAndTables(createTableStmts)
	if sqlQuery == "" {
		return fmt.Errorf("no SQL statements to execute")
	}
	return t.mysqlService.ExecuteInTxn(sqlQuery)
}

func (t *TargetMySQLService) getBranchMeta(name string) *BranchService {
	// todo
	return nil
}

// todo branch: snapshot and branchMeta should have their struct type
func (t *TargetMySQLService) storeBranchMeta(snapshot map[string]map[string]string, branchMeta BranchMeta) error {
	// todo
	//insertSnapshotSQL := getInsertSnapshotSQL(branchMeta.name, string(snapshot))
	//insertBranchMetaSQL := getInsertBranchMetaSQL(branchMeta.name, sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude, "create", "")

	// todo: need to call: checkBranchMetaExists

	return nil
}

func (t *TargetMySQLService) checkBranchMetaExists(name string) bool {
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
