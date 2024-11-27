package branch

import (
	"fmt"
	"regexp"
	"strings"
)

type TargetMySQLService struct {
	mysqlService *MysqlService
}

var CreateTablesBatchSize = 10
var InsertSnapshotBatchSize = 10
var InsertMergeBackDDLBatchSize = 10

func (t *TargetMySQLService) SelectOrInsertBranchMeta(metaToInsertIfNotExists *BranchMeta) (*BranchMeta, error) {

	meta, _ := t.selectBranchMeta(metaToInsertIfNotExists.name)
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
// todo param to name
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

// todo, 组合
// todo, it's exactly the same as SourceMySQLService.GetBranchSchema, move it to a common place
// GetBranchSchema retrieves CREATE TABLE statements for all tables in databases filtered by `databasesInclude` and `databasesExclude`
func (t *TargetMySQLService) GetBranchSchema(databasesInclude, databasesExclude []string) (*BranchSchema, error) {
	tableInfos, err := t.getTableInfos(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}
	return t.getBranchSchemaInBatches(tableInfos, GetBranchSchemaBatchSize)
}

/**********************************************************************************************************************/

// todo, it's exactly the same as SourceMySQLService.getTableInfos, move it to a common place
// getTableInfos executes the table info query and returns a slice of tableInfo
func (t *TargetMySQLService) getTableInfos(databasesInclude, databasesExclude []string) ([]TableInfo, error) {
	query, err := buildTableInfosQuerySQL(databasesInclude, databasesExclude)
	if err != nil {
		return nil, err
	}

	rows, err := t.mysqlService.Query(query)
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

// todo it's exactly the same as SourceMySQLService.getBranchSchemaInBatches, move it to a common place
// getBranchSchemaInBatches retrieves CREATE TABLE statements in batches
func (t *TargetMySQLService) getBranchSchemaInBatches(tableInfos []TableInfo, batchSize int) (*BranchSchema, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < len(tableInfos); i += batchSize {
		end := i + batchSize
		if end > len(tableInfos) {
			end = len(tableInfos)
		}
		batch := tableInfos[i:end]

		combinedQuery := getCombinedShowCreateTableSQL(batch)

		// Execute the combined query
		multiRows, err := t.mysqlService.Query(combinedQuery)
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

func (t *TargetMySQLService) getSnapshot(name string) (*BranchSchema, error) {
	selectSnapshotSQL := getSelectSnapshotSQL(name)
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
			id             int64
			name           string
			database       string
			table          string
			createTableSQL string
		)

		if err := rows.Scan(&id, &name, &database, &table, &createTableSQL); err != nil {
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
		return nil, fmt.Errorf("no snapshot found for name: %s", name)
	}

	return result, nil
}

func (t *TargetMySQLService) deleteSnapshot(name string) error {
	deleteBranchSnapshotSQL := getDeleteSnapshotSQL(name)
	_, err := t.mysqlService.Exec(deleteBranchSnapshotSQL)
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
	_, err := t.mysqlService.Exec(deleteBranchMergeBackSQL)
	return err
}

func (t *TargetMySQLService) insertMergeBackDDLInBatches(name string, ddls *BranchDiff, batchSize int) error {
	insertSQLs := make([]string, 0)
	for database, databaseDiff := range ddls.diffs {
		if databaseDiff.needDropDatabase {
			insertSQLs = append(insertSQLs, fmt.Sprintf("DROP DATABASE IF EXISTS %s", database))
			continue
		}

		if databaseDiff.needCreateDatabase {
			insertSQLs = append(insertSQLs, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
		}

		for tableName, ddls := range databaseDiff.tableDDLs {
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

func (t *TargetMySQLService) createDatabaseAndTables(branchSchema *BranchSchema) error {
	for database, tables := range branchSchema.branchSchema {
		// create database
		_, err := t.mysqlService.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))
		if err != nil {
			return fmt.Errorf("failed to create database '%s': %v", database, err)
		}

		// create tables in batches
		createTableStmts := addIfNotExistsForCreateTableSQL(tables)
		err = t.createTablesInBatches(database, createTableStmts, CreateTablesBatchSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TargetMySQLService) createTablesInBatches(databaseName string, createTableStmts map[string]string, batchSize int) error {
	if batchSize <= 0 {
		return fmt.Errorf("invalid batch size: %d", batchSize)
	}

	stmts := make([]string, 0, len(createTableStmts))
	for _, stmt := range createTableStmts {
		stmt = strings.TrimSpace(stmt)
		stmt = strings.TrimSuffix(stmt, ";")
		stmts = append(stmts, stmt)
	}

	for i := 0; i < len(stmts); i += batchSize {
		end := i + batchSize
		if end > len(stmts) {
			end = len(stmts)
		}

		batchSQL := strings.Join(stmts[i:end], ";")
		// todo: remove use?
		batchSQL = fmt.Sprintf("USE %s; %s", databaseName, batchSQL)

		if _, err := t.mysqlService.Exec(batchSQL); err != nil {
			return fmt.Errorf("failed to execute batch create tables %s: %v", batchSQL, err)
		}
	}

	return nil
}

func (t *TargetMySQLService) selectBranchMeta(name string) (*BranchMeta, error) {
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

	err = rows.Scan(
		&meta.name,
		&meta.sourceHost,
		&meta.sourcePort,
		&meta.sourceUser,
		&meta.sourcePassword,
		&includeDBs,
		&excludeDBs,
		&meta.targetDBPattern,
		&status,
	)
	if err != nil {
		return nil, err
	}

	// todo enhancement: add param verification

	if includeDBs == "" {
		meta.includeDatabases = []string{}
	} else {
		meta.includeDatabases = strings.Split(includeDBs, ",")
	}
	if excludeDBs == "" {
		meta.excludeDatabases = []string{}
	} else {
		meta.excludeDatabases = strings.Split(excludeDBs, ",")
	}

	meta.status = StringToBranchStatus(status)
	return &meta, nil
}

func (t *TargetMySQLService) UpsertBranchMeta(branchMeta *BranchMeta) error {
	sql := getUpsertBranchMetaSQL(branchMeta)
	_, err := t.mysqlService.Exec(sql)
	return err
}

func (t *TargetMySQLService) InsertBranchMeta(branchMeta *BranchMeta) error {
	sql := getInsertBranchMetaSQL(branchMeta)
	_, err := t.mysqlService.Exec(sql)
	return err
}

// branch meta related

func getSelectBranchMetaSQL(name string) string {
	return fmt.Sprintf(SelectBranchMetaSQL, name)
}

func getUpsertBranchMetaSQL(branchMeta *BranchMeta) string {
	includeDatabases := strings.Join(branchMeta.includeDatabases, ",")
	excludeDatabases := strings.Join(branchMeta.excludeDatabases, ",")
	return fmt.Sprintf(UpsertBranchMetaSQL,
		branchMeta.name,
		branchMeta.sourceHost,
		branchMeta.sourcePort,
		branchMeta.sourceUser,
		branchMeta.sourcePassword,
		includeDatabases,
		excludeDatabases,
		string(branchMeta.status),
		branchMeta.targetDBPattern)
}

func getInsertBranchMetaSQL(branchMeta *BranchMeta) string {
	includeDatabases := strings.Join(branchMeta.includeDatabases, ",")
	excludeDatabases := strings.Join(branchMeta.excludeDatabases, ",")
	return fmt.Sprintf(InsertBranchMetaSQL,
		branchMeta.name,
		branchMeta.sourceHost,
		branchMeta.sourcePort,
		branchMeta.sourceUser,
		branchMeta.sourcePassword,
		includeDatabases,
		excludeDatabases,
		string(branchMeta.status),
		branchMeta.targetDBPattern)
}

// snapshot related

func getSelectSnapshotSQL(name string) string {
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

func getUpdateDDLMergedSQL(id int) string {
	return fmt.Sprintf(UpdateBranchMergeBackDDLMergedSQL, id)
}
