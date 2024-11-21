package branch

import (
	"encoding/json"
	"fmt"
)

type TargetMySQLService struct {
	mysqlService *MysqlService
}

func (t *TargetMySQLService) CreateDatabaseAndTablesIfNotExists(createTableStmts *BranchSchema) error {
	// get databases from target
	databases, err := t.getAllDatabases()
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(createTableStmts.schema, db)
	}

	// apply schema to target
	err = t.createDatabaseAndTables(createTableStmts.schema)
	if err != nil {
		return err
	}
	return nil
}

func (t *TargetMySQLService) StoreBranchMeta(snapshot *BranchSchema, branchMeta *BranchMeta) error {
	snapshotJson, err := json.Marshal(snapshot.schema)
	if err != nil {
		return err
	}
	insertBranchMetaSQL := getInsertBranchMetaSQL(branchMeta)
	insertSnapshotSQL := getInsertSnapshotSQL(branchMeta.name, string(snapshotJson))

	// if the branch name has existed, the sql will fail
	return t.mysqlService.ExecuteInTxn(insertBranchMetaSQL, insertSnapshotSQL)
}

/**********************************************************************************************************************/

// getAllDatabases retrieves all database names from MySQL
func (t *TargetMySQLService) getAllDatabases() ([]string, error) {
	// Execute query to get all database names
	rows, err := t.mysqlService.Query("SHOW DATABASES")
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

func getInsertSnapshotSQL(name, snapshotData string) string {
	return fmt.Sprintf(InsertBranchSnapshotSQL, name, snapshotData)
}

func getInsertBranchMetaSQL(branchMeta *BranchMeta) string {
	return fmt.Sprintf(InsertBranchSQL,
		branchMeta.name,
		branchMeta.sourceHost,
		branchMeta.sourcePort,
		branchMeta.sourceUser,
		branchMeta.sourcePassword,
		branchMeta.include,
		branchMeta.exclude,
		branchMeta.status,
		branchMeta.targetDBPattern)
}
