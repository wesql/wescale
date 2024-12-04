package branch

import (
	"fmt"
	"regexp"
	"strings"
)

type TargetMySQLService struct {
	*CommonMysqlService
	mysqlService *MysqlService
}

func NewTargetMySQLService(mysqlService *MysqlService) *TargetMySQLService {
	return &TargetMySQLService{
		CommonMysqlService: &CommonMysqlService{
			mysqlService: mysqlService,
		},
		mysqlService: mysqlService,
	}
}

var InsertSnapshotBatchSize = 10
var InsertMergeBackDDLBatchSize = 10

func (t *TargetMySQLService) SelectOrInsertBranchMeta(metaToInsertIfNotExists *BranchMeta) (*BranchMeta, error) {

	meta, _ := t.SelectAndValidateBranchMeta(metaToInsertIfNotExists.Name)
	if meta != nil {
		return meta, nil
	}

	err := t.InsertBranchMeta(metaToInsertIfNotExists)
	if err != nil {
		return nil, err
	}
	return metaToInsertIfNotExists, nil
}

// ApplySnapshot applies the stored snapshot schema to the target MySQL instance.
//
// Idempotency:
// The function is idempotent through:
// - Using 'CREATE DATABASE IF NOT EXISTS' and 'CREATE TABLE IF NOT EXISTS' commands
//
// Note:
// Existing databases in the target instance are intentionally skipped.
//
// Parameters:
// - meta: Contains branch metadata and configuration
//
// Returns:
// - error: Returns nil on success, error otherwise
func (t *TargetMySQLService) ApplySnapshot(name string) error {

	// get databases from target
	databases, err := t.getAllDatabases()
	if err != nil {
		return err
	}

	snapshot, err := t.getSnapshot(name)
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(snapshot.branchSchema, db)
	}

	// apply schema to target
	err = t.createDatabaseAndTables(snapshot)
	if err != nil {
		return err
	}

	return nil
}

/**********************************************************************************************************************/

func (t *TargetMySQLService) GetMysqlService() *MysqlService {
	return t.mysqlService
}

func (t *TargetMySQLService) getSnapshot(name string) (*BranchSchema, error) {
	selectSnapshotSQL := GetSelectSnapshotSQL(name)
	// mysql Query will stream the result, so we don't need to worry if the data is too large to transfer.
	rows, err := t.mysqlService.Query(selectSnapshotSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %v: %v", selectSnapshotSQL, err)
	}
	defer rows.Close()

	result := &BranchSchema{
		branchSchema: make(map[string]map[string]string),
	}

	for rows.Next() {
		var (
			id              int64
			name            string
			database        string
			table           string
			createTableSQL  string
			updateTimestamp string
		)

		if err := rows.Scan(&id, &name, &database, &table, &createTableSQL, &updateTimestamp); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		if _, ok := result.branchSchema[database]; !ok {
			result.branchSchema[database] = make(map[string]string)
		}

		result.branchSchema[database][table] = createTableSQL
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %v", err)
	}

	if len(result.branchSchema) == 0 {
		return nil, fmt.Errorf("no snapshot found for Name: %s", name)
	}

	return result, nil
}

func (t *TargetMySQLService) deleteSnapshot(name string) error {
	deleteBranchSnapshotSQL := getDeleteSnapshotSQL(name)
	_, err := t.mysqlService.Exec("", deleteBranchSnapshotSQL)
	return err
}

func (t *TargetMySQLService) insertSnapshotInBatches(name string, schema *BranchSchema, batchSize int) error {
	insertSQLs := make([]string, 0)
	for database, tables := range schema.branchSchema {
		for tableName, createTableSQL := range tables {
			sql := getInsertSnapshotSQL(name, database, tableName, createTableSQL)
			insertSQLs = append(insertSQLs, sql)
		}
	}
	for i := 0; i < len(insertSQLs); i += batchSize {
		endIndex := i + batchSize
		if endIndex > len(insertSQLs) {
			endIndex = len(insertSQLs)
		}
		err := t.mysqlService.ExecuteInTxn(insertSQLs[i:endIndex]...)
		if err != nil {
			return fmt.Errorf("failed to insert snapshot %v: %v", insertSQLs[i:endIndex], err)
		}
	}
	return nil
}

func (t *TargetMySQLService) deleteMergeBackDDL(name string) error {
	deleteBranchMergeBackSQL := getDeleteMergeBackDDLSQL(name)
	_, err := t.mysqlService.Exec("", deleteBranchMergeBackSQL)
	return err
}

func (t *TargetMySQLService) insertMergeBackDDLInBatches(name string, ddls *BranchDiff, batchSize int) error {
	insertSQLs := make([]string, 0)
	for database, databaseDiff := range ddls.Diffs {
		if databaseDiff.NeedDropDatabase {
			insertSQLs = append(insertSQLs, fmt.Sprintf("DROP DATABASE IF EXISTS %s", database))
			continue
		}

		if databaseDiff.NeedCreateDatabase {
			insertSQLs = append(insertSQLs, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
		}

		for tableName, ddls := range databaseDiff.TableDDLs {
			for _, ddl := range ddls {
				sql := getInsertMergeBackDDLSQL(name, database, tableName, ddl)
				insertSQLs = append(insertSQLs, sql)
			}
		}
	}
	for i := 0; i < len(insertSQLs); i += batchSize {
		endIndex := i + batchSize
		if endIndex > len(insertSQLs) {
			endIndex = len(insertSQLs)
		}
		err := t.mysqlService.ExecuteInTxn(insertSQLs[i:endIndex]...)
		if err != nil {
			return fmt.Errorf("failed to insert ddl %v: %v", insertSQLs[i:endIndex], err)
		}
	}
	return nil
}

// addIfNotExistsForCreateTableSQL modifies CREATE TABLE statements to include IF NOT EXISTS clause.
//
// Parameters:
// - tables: A map where keys are table names and values are CREATE TABLE statements
//
// Returns:
// - map[string]string: A new map with modified CREATE TABLE statements including IF NOT EXISTS
//
// Example:
// Input:  "CREATE TABLE users (...)"
// Output: "CREATE TABLE IF NOT EXISTS users (...)"
func addIfNotExistsForCreateTableSQL(tables map[string]string) map[string]string {
	// Compile regex pattern to match CREATE TABLE statements
	// (?i) makes the match case-insensitive
	// \s+ matches one or more whitespace characters
	// (?:...)? is for optional IF NOT EXISTS part
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)

	result := make(map[string]string, len(tables))

	for tableName, createSQL := range tables {
		// Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
		modifiedSQL := re.ReplaceAllString(createSQL,
			"CREATE TABLE IF NOT EXISTS ")

		result[tableName] = modifiedSQL
	}

	return result
}

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
			return nil, fmt.Errorf("failed to scan database Name: %v", err)
		}
		databases = append(databases, dbName)
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating database list: %v", err)
	}

	return databases, nil
}

func (t *TargetMySQLService) createDatabaseAndTables(branchSchema *BranchSchema) error {
	for database, tables := range branchSchema.branchSchema {
		// create database
		_, err := t.mysqlService.Exec("", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))
		if err != nil {
			return fmt.Errorf("failed to create database '%s': %v", database, err)
		}

		// create tables
		createTableStmts := addIfNotExistsForCreateTableSQL(tables)
		err = t.createTables(database, createTableStmts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TargetMySQLService) createTables(databaseName string, createTableStmts map[string]string) error {
	for _, sql := range createTableStmts {
		if _, err := t.mysqlService.Exec(databaseName, sql); err != nil {
			return fmt.Errorf("failed to execute create tables %s: %v", sql, err)
		}
	}
	return nil
}

func (t *TargetMySQLService) SelectAndValidateBranchMeta(name string) (*BranchMeta, error) {
	selectBranchMetaSQL := getSelectBranchMetaSQL(name)
	rows, err := t.mysqlService.Query(selectBranchMetaSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, err
	}

	var meta BranchMeta
	var includeDBs, excludeDBs, status string
	var id int

	err = rows.Scan(
		&id,
		&meta.Name,
		&meta.SourceHost,
		&meta.SourcePort,
		&meta.SourceUser,
		&meta.SourcePassword,
		&includeDBs,
		&excludeDBs,
		&meta.TargetDBPattern,
		&status,
	)
	if err != nil {
		return nil, err
	}

	if includeDBs == "" {
		meta.IncludeDatabases = []string{}
	} else {
		meta.IncludeDatabases = strings.Split(includeDBs, ",")
	}
	if excludeDBs == "" {
		meta.ExcludeDatabases = []string{}
	} else {
		meta.ExcludeDatabases = strings.Split(excludeDBs, ",")
	}

	meta.Status = StringToBranchStatus(status)
	err = meta.Validate()
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (t *TargetMySQLService) UpsertBranchMeta(branchMeta *BranchMeta) error {
	sql := getUpsertBranchMetaSQL(branchMeta)
	_, err := t.mysqlService.Exec("", sql)
	return err
}

func (t *TargetMySQLService) InsertBranchMeta(branchMeta *BranchMeta) error {
	sql := getInsertBranchMetaSQL(branchMeta)
	_, err := t.mysqlService.Exec("", sql)
	return err
}

func (t *TargetMySQLService) UpdateBranchStatus(name string, status BranchStatus) error {
	sql := getUpdateBranchStatusSQL(name, status)
	_, err := t.mysqlService.Exec("", sql)
	return err
}

// branch meta related

func getSelectBranchMetaSQL(name string) string {
	return fmt.Sprintf(SelectBranchMetaSQL, name)
}

func getUpsertBranchMetaSQL(branchMeta *BranchMeta) string {
	includeDatabases := strings.Join(branchMeta.IncludeDatabases, ",")
	excludeDatabases := strings.Join(branchMeta.ExcludeDatabases, ",")
	return fmt.Sprintf(UpsertBranchMetaSQL,
		branchMeta.Name,
		branchMeta.SourceHost,
		branchMeta.SourcePort,
		branchMeta.SourceUser,
		branchMeta.SourcePassword,
		includeDatabases,
		excludeDatabases,
		string(branchMeta.Status),
		branchMeta.TargetDBPattern)
}

func getInsertBranchMetaSQL(branchMeta *BranchMeta) string {
	includeDatabases := strings.Join(branchMeta.IncludeDatabases, ",")
	excludeDatabases := strings.Join(branchMeta.ExcludeDatabases, ",")
	return fmt.Sprintf(InsertBranchMetaSQL,
		branchMeta.Name,
		branchMeta.SourceHost,
		branchMeta.SourcePort,
		branchMeta.SourceUser,
		branchMeta.SourcePassword,
		includeDatabases,
		excludeDatabases,
		string(branchMeta.Status),
		branchMeta.TargetDBPattern)
}

func getUpdateBranchStatusSQL(name string, status BranchStatus) string {
	return fmt.Sprintf(UpdateBranchStatusSQL, string(status), name)
}

func getDeleteBranchMetaSQL(name string) string {
	return fmt.Sprintf(DeleteBranchMetaSQL, name)
}

// snapshot related

func GetSelectSnapshotSQL(name string) string {
	return fmt.Sprintf(SelectBranchSnapshotSQL, name)
}

func getDeleteSnapshotSQL(name string) string {
	return fmt.Sprintf(DeleteBranchSnapshotSQL, name)
}

func getInsertSnapshotSQL(name, database, table, createTable string) string {
	return fmt.Sprintf(InsertBranchSnapshotSQL, name, database, table, createTable)
}

// merge back ddl related

func getDeleteMergeBackDDLSQL(name string) string {
	return fmt.Sprintf(DeleteBranchMergeBackDDLSQL, name)
}

func getInsertMergeBackDDLSQL(name, database, table, ddl string) string {
	return fmt.Sprintf(InsertBranchMergeBackDDLSQL, name, database, table, ddl)
}

func getSelectUnmergedDDLSQL(name string) string {
	return fmt.Sprintf(SelectBranchUnmergedDDLSQL, name)
}

func GetSelectMergeBackDDLSQL(name string) string {
	return fmt.Sprintf(SelectBranchMergeBackDDLSQL, name)
}

func getUpdateDDLMergedSQL(id int) string {
	return fmt.Sprintf(UpdateBranchMergeBackDDLMergedSQL, id)
}
