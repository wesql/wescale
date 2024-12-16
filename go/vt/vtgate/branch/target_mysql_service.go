package branch

import (
	"fmt"
	"github.com/pingcap/failpoint"
	"regexp"
	"strings"
	"vitess.io/vitess/go/vt/failpointkey"
	"vitess.io/vitess/go/vt/sqlparser"
)

type TargetMySQLService struct {
	*CommonMysqlService
	mysqlService MysqlService
}

func NewTargetMySQLService(mysqlService MysqlService) *TargetMySQLService {
	return &TargetMySQLService{
		CommonMysqlService: &CommonMysqlService{
			mysqlService: mysqlService,
		},
		mysqlService: mysqlService,
	}
}

const (
	InsertSnapshotBatchSize     = 10
	InsertMergeBackDDLBatchSize = 10
)

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
	failpoint.Inject(failpointkey.BranchApplySnapshotError.Name, func() {
		failpoint.Return(fmt.Errorf("error applying snapshot by failpoint"))
	})
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

func (t *TargetMySQLService) GetMysqlService() MysqlService {
	return t.mysqlService
}

/**********************************************************************************************************************/

func (t *TargetMySQLService) getSnapshot(name string) (*BranchSchema, error) {
	result := &BranchSchema{
		branchSchema: make(map[string]map[string]string),
	}
	lastID := -1
	for {
		selectSnapshotSQL := GetSelectSnapshotInBatchSQL(name, lastID, SelectBatchSize)
		rows, err := t.mysqlService.Query(selectSnapshotSQL)
		if err != nil {
			return nil, fmt.Errorf("failed to query snapshot %v: %v", selectSnapshotSQL, err)
		}

		for _, row := range rows {

			database := BytesToString(row.RowData["database"])
			table := BytesToString(row.RowData["table"])
			createTableSQL := BytesToString(row.RowData["create_table_sql"])

			if _, ok := result.branchSchema[database]; !ok {
				result.branchSchema[database] = make(map[string]string)
			}

			result.branchSchema[database][table] = createTableSQL
		}

		if len(rows) < SelectBatchSize {
			break
		}
		lastID, _ = BytesToInt(rows[len(rows)-1].RowData["id"])
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
			normalizedSQL, err := normalizeCreateTableSQL(createTableSQL)
			if err != nil {
				return err
			}
			sql := getInsertSnapshotSQL(name, database, tableName, normalizedSQL)
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

func normalizeCreateTableSQL(createTableSQL string) (string, error) {
	s, err := sqlparser.Parse(createTableSQL)
	if err != nil {
		return "", err
	}
	createStmt := s.(*sqlparser.CreateTable)
	createStmt.IfNotExists = true
	// remove engine information
	tmp := make([]*sqlparser.TableOption, 0)
	for _, opt := range createStmt.TableSpec.Options {
		if opt.Name != "ENGINE" {
			tmp = append(tmp, opt)
		}
	}
	createStmt.TableSpec.Options = tmp

	return sqlparser.String(createStmt), nil
}

func (t *TargetMySQLService) deleteMergeBackDDL(name string) error {
	deleteBranchMergeBackSQL := getDeleteMergeBackDDLSQL(name)
	_, err := t.mysqlService.Exec("", deleteBranchMergeBackSQL)
	return err
}

func (t *TargetMySQLService) insertMergeBackDDLInBatches(name string, ddls *BranchDiff, batchSize int) error {
	failpoint.Inject(failpointkey.BranchInsertMergeBackDDLError.Name, func() {
		failpoint.Return(fmt.Errorf("error inserting merge back ddl by failpoint"))
	})
	insertSQLs := make([]string, 0)
	for database, databaseDiff := range ddls.Diffs {
		if databaseDiff.NeedDropDatabase {
			ddl := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database)
			sql := getInsertMergeBackDDLSQL(name, database, "", ddl)
			insertSQLs = append(insertSQLs, sql)
			continue
		}

		if databaseDiff.NeedCreateDatabase {
			ddl := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
			sql := getInsertMergeBackDDLSQL(name, database, "", ddl)
			insertSQLs = append(insertSQLs, sql)
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

	var databases []string
	for _, row := range rows {
		dbName := BytesToString(row.RowData["Database"])
		databases = append(databases, dbName)
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
		err = t.createTables(database, tables)
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

	if len(rows) == 0 {
		return nil, fmt.Errorf("branch not found: %s", name)
	}

	var meta BranchMeta
	var includeDBs, excludeDBs, status string

	meta.Name = BytesToString(rows[0].RowData["name"])
	meta.SourceHost = BytesToString(rows[0].RowData["source_host"])
	meta.SourcePort, _ = BytesToInt(rows[0].RowData["source_port"])
	meta.SourceUser = BytesToString(rows[0].RowData["source_user"])
	meta.SourcePassword = BytesToString(rows[0].RowData["source_password"])
	includeDBs = BytesToString(rows[0].RowData["include_databases"])
	excludeDBs = BytesToString(rows[0].RowData["exclude_databases"])
	status = BytesToString(rows[0].RowData["status"])

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
		string(branchMeta.Status))
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
		string(branchMeta.Status))
}

func getUpdateBranchStatusSQL(name string, status BranchStatus) string {
	return fmt.Sprintf(UpdateBranchStatusSQL, string(status), name)
}

func getDeleteBranchMetaSQL(name string) string {
	return fmt.Sprintf(DeleteBranchMetaSQL, name)
}

// snapshot related

func GetSelectSnapshotInBatchSQL(name string, id, batchSize int) string {
	return fmt.Sprintf(SelectBranchSnapshotInBatchSQL, name, id, batchSize)
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

func getSelectUnmergedDDLInBatchSQL(name string, id, batchSize int) string {
	return fmt.Sprintf(SelectBranchUnmergedDDLInBatchSQL, name, id, batchSize)
}

func getSelectUnmergedDBDDLInBatchSQL(name string, id, batchSize int) string {
	return fmt.Sprintf(SelectBranchUnmergedDBDDLInBatchSQL, name, id, batchSize)
}

func GetSelectMergeBackDDLInBatchSQL(name string, id, batchSize int) string {
	return fmt.Sprintf(SelectBranchMergeBackDDLInBatchSQL, name, id, batchSize)
}

func getUpdateDDLMergedSQL(id int) string {
	return fmt.Sprintf(UpdateBranchMergeBackDDLMergedSQL, id)
}
