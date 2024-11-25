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

func (t *TargetMySQLService) SelectOrInsertBranchMeta(metaToInsertIfNotExists *BranchMeta) (*BranchMeta, error) {

	meta, _ := t.selectBranchMeta(metaToInsertIfNotExists.name)
	if meta != nil {
		return meta, nil
	}

	err := t.UpsertBranchMeta(metaToInsertIfNotExists)
	if err != nil {
		return nil, err
	}
	return metaToInsertIfNotExists, nil
}

// todo make it idempotent
// todo comment
// 幂等性：确保将当前存在表中的snapshot应用到目标端，借助create database if not exist 和 create table if not exist这两个命令的幂等性，
// 注意：会忽略掉已经在目标端的数据库
func (t *TargetMySQLService) ApplySnapshot(meta *BranchMeta) error {

	// get databases from target
	databases, err := t.getAllDatabases()
	if err != nil {
		return err
	}

	snapshot, err := t.getSnapshot(meta)
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(snapshot.schema, db)
	}

	// apply schema to target
	err = t.createDatabaseAndTables(snapshot)
	if err != nil {
		return err
	}

	return nil
}

/**********************************************************************************************************************/

func (t *TargetMySQLService) getSnapshot(meta *BranchMeta) (*BranchSchema, error) {
	selectSnapshotSQL := getSelectSnapshotSQL(meta.name)
	// mysql Query will stream the result, so we don't need to worry if the data is too large to transfer.
	rows, err := t.mysqlService.Query(selectSnapshotSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %v: %v", selectSnapshotSQL, err)
	}
	defer rows.Close()

	result := &BranchSchema{
		schema: make(map[string]map[string]string),
	}

	for rows.Next() {
		var (
			id             int64
			name           string
			database       string
			table          string
			createTableSQL string
			schemaType     string
		)

		if err := rows.Scan(&id, &name, &database, &table, &createTableSQL, &schemaType); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		if _, ok := result.schema[database]; !ok {
			result.schema[database] = make(map[string]string)
		}

		result.schema[database][table] = createTableSQL
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during rows iteration: %v", err)
	}

	if len(result.schema) == 0 {
		return nil, fmt.Errorf("no snapshot found for name: %s", meta.name)
	}

	return result, nil
}

func (t *TargetMySQLService) deleteSnapshot(branchMeta *BranchMeta) error {
	deleteBranchSnapshotSQL := getDeleteSnapshotSQL(branchMeta.name)
	_, err := t.mysqlService.Exec(deleteBranchSnapshotSQL)
	return err
}

func (t *TargetMySQLService) insertSnapshotInBatches(meta *BranchMeta, schema *BranchSchema, batchSize int) error {
	insertSQLs := make([]string, 0)
	for database, tables := range schema.schema {
		for tableName, createTableSQL := range tables {
			sql := getInsertSnapshotSQL(meta.name, database, tableName, createTableSQL)
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

// todo add test case
// param tables and return: map tableName -> create table sql
func addIfNotExistsForCreateTableSQL(tables map[string]string) map[string]string {
	// 编译正则表达式，匹配 CREATE TABLE 语句
	// (?i) 使匹配大小写不敏感
	// \s+ 匹配一个或多个空白字符
	// (?:...)? 用于可选的 IF NOT EXISTS 部分
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)

	result := make(map[string]string, len(tables))

	for tableName, createSQL := range tables {
		// 替换 CREATE TABLE 语句为 CREATE TABLE IF NOT EXISTS
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

// 幂等性：确保创建输入参数中的数据库和表，忽略已经存在的数据库和表
func (t *TargetMySQLService) createDatabaseAndTables(branchSchema *BranchSchema) error {
	for database, tables := range branchSchema.schema {
		// create database
		_, err := t.mysqlService.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
		if err != nil {
			return fmt.Errorf("failed to create database '%s': %v", database, err)
		}

		// create tables in batch
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
		batchSQL = fmt.Sprintf("USE %s; %s", databaseName, batchSQL)

		if _, err := t.mysqlService.Exec(batchSQL); err != nil {
			return fmt.Errorf("failed to execute batch create tables %s: %v", batchSQL, err)
		}
	}

	return nil
}

func (t *TargetMySQLService) selectBranchStatus(name string) (BranchStatus, error) {
	meta, err := t.selectBranchMeta(name)
	if err != nil {
		return StatusUnknown, err
	}
	return meta.status, nil
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

func getSelectSnapshotSQL(name string) string {
	return fmt.Sprintf(SelectBranchSnapshotSQL, name)
}

func getDeleteSnapshotSQL(name string) string {
	return fmt.Sprintf(DeleteBranchSnapshotSQL, name)
}

func getInsertSnapshotSQL(name, database, table, createTable string) string {
	return fmt.Sprintf(InsertBranchSnapshotSQL, name, database, table, createTable)
}

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
