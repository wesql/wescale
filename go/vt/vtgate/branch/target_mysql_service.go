package branch

import (
	"fmt"
	"regexp"
	"strings"
)

type TargetMySQLService struct {
	mysqlService *MysqlService
}

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

// todo complete me
// Query接口本身就是流式的
func (t *TargetMySQLService) getSnapshot(meta *BranchMeta) (*BranchSchema, error) {
	return nil, nil
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

// todo complete me
func (t *TargetMySQLService) createDatabaseAndTables(branchSchema *BranchSchema) error {
	//sqlQuery := addIfNotExistsForCreateTableSQL(branchSchema)
	//if sqlQuery == "" {
	//	return fmt.Errorf("no SQL statements to execute")
	//}
	//return t.mysqlService.ExecuteInTxn(sqlQuery)
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
	row, err := t.mysqlService.Query(sql)
	if err != nil {
		return err
	}
	defer row.Close()
	return nil
}

func getInsertSnapshotSQL(name, snapshotData string) string {
	// todo fix me, snapshot spliting
	//return fmt.Sprintf(InsertBranchSnapshotSQL, name, snapshotData)
	return ""
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
