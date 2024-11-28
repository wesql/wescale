package branch

import (
	"fmt"
	"strings"
	"vitess.io/vitess/go/vt/schemadiff"
)

var (
	DefaultExcludeDatabases = []string{"mysql", "sys", "information_schema", "performance_schema"}
)

type BranchService struct {
	sourceMySQLService *SourceMySQLService
	targetMySQLService *TargetMySQLService
}

func NewBranchService(sourceHandler *SourceMySQLService, targetHandler *TargetMySQLService) *BranchService {
	return &BranchService{
		sourceMySQLService: sourceHandler,
		targetMySQLService: targetHandler,
	}
}

func NewBranchMeta(name, sourceHost string, sourcePort int, sourceUser, sourcePassword,
	includeDBs, excludeDBs, targetDBPattern string) (*BranchMeta, error) {

	var includeDatabases []string
	if includeDBs == "" {
		return nil, fmt.Errorf("includeDatabases cannot be empty")
	}

	includeDatabases = strings.Split(includeDBs, ",")
	for i, db := range includeDatabases {
		if db == "*" {
			includeDatabases = []string{"*"}
			break
		}
		includeDatabases[i] = strings.TrimSpace(db)
	}

	var excludeDatabases []string
	if excludeDBs != "" {
		excludeDatabases = strings.Split(excludeDBs, ",")
		for i, db := range excludeDatabases {
			db = strings.TrimSpace(db)
			if db == "*" {
				return nil, fmt.Errorf("excludeDatabases contains wildcard '*', branching is meaningless")
			}
			excludeDatabases[i] = db
		}
	}

	if name == "" {
		return nil, fmt.Errorf("name cannot be empty")
	}
	if sourceHost == "" {
		return nil, fmt.Errorf("sourceHost cannot be empty")
	}
	if sourcePort <= 0 || sourcePort > 65535 {
		return nil, fmt.Errorf("invalid sourcePort: %d", sourcePort)
	}

	bMeta := &BranchMeta{
		name:             name,
		sourceHost:       sourceHost,
		sourcePort:       sourcePort,
		sourceUser:       sourceUser,
		sourcePassword:   sourcePassword,
		includeDatabases: includeDatabases,
		excludeDatabases: excludeDatabases,
		targetDBPattern:  targetDBPattern,
		status:           StatusInit,
	}

	addDefaultExcludeDatabases(bMeta)
	return bMeta, nil
}

// BranchCreate creates a database branch in the target MySQL instance based on the source schema.
// It filters the schema according to include/exclude rules and saves it as a snapshot.
// The function will skip databases that already exist in the target instance.
//
// State Transitions:
// - Init -> Fetched -> Created
// - Starts in Init state
// - Moves to Fetched state after successfully pulling and saving the snapshot
// - Reaches Created state after applying the snapshot to the target
//
// Idempotency:
// This function is idempotent and can safely handle interruptions:
// - If the branch hasn't been created, it will create it
// - If interrupted during creation, subsequent runs will continue from the last successful state
// - If the branch already exists, it returns successfully without further operations
//
// Parameters:
// - branchMeta: Contains the branch metadata and configuration
//
// Returns:
// - error: Returns nil on success, error otherwise
// todo enhancement: filter schemas about table gc and online DDL shadow tables
func (bs *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	if branchMeta.status != StatusInit {
		return fmt.Errorf("the status of branch meta should be init")
	}
	meta, err := bs.targetMySQLService.SelectOrInsertBranchMeta(branchMeta)
	if err != nil {
		return err
	}
	if meta.status == StatusInit || meta.status == StatusUnknown {
		_, err := bs.branchFetchSnapshot(meta.name, meta.includeDatabases, meta.excludeDatabases)
		if err != nil {
			return err
		}
		// upsert make sure the meta stored in mysql will be synced
		meta.status = StatusFetched
		err = bs.targetMySQLService.UpsertBranchMeta(meta)
		if err != nil {
			return err
		}
	}

	if meta.status == StatusFetched {
		err := bs.targetMySQLService.ApplySnapshot(meta.name)
		if err != nil {
			return err
		}
		meta.status = StatusCreated
		return bs.targetMySQLService.UpsertBranchMeta(meta)
	}

	return nil
}

// BranchDiff calculates schema differences between selected objects based on the provided parameters.
// It supports comparing schemas between three types of objects: source, target, and snapshot.
//
// The function assumes that the input branchMeta is valid, including its include/exclude fields,
// and therefore does not perform additional parameter validation.
//
// Schema Retrieval:
// - Source and target schemas are fetched via real-time queries
// - Snapshot schema is retrieved from entries stored in the target instance
// - Returns error if the requested snapshot doesn't exist
//
// Supported Comparison Modes (branchDiffObjectsFlag):
// - FromSourceToTarget/ FromTargetToSource: Compares source MySQL schema with target MySQL schema
// - FromTargetToSnapshot/ FromSnapshotToTarget: Compares target MySQL schema with stored snapshot
// - FromSnapshotToSource/ FromSourceToSnapshot: Compares stored snapshot with source MySQL schema
//
// Parameters:
// - branchMeta: Contains branch configuration including database filters
// - branchDiffObjectsFlag: Specifies which objects to compare and comparison direction
// - hints: Additional configuration for diff calculation
//
// Returns:
// - *BranchDiff: Contains the calculated schema differences
// - error: Returns nil on success, error on invalid flag or retrieval failure
// todo enhancement: filter schemas about table gc and online DDL shadow tables
// todo branchMeta -> name
func (bs *BranchService) BranchDiff(name string, includeDatabases, excludeDatabases []string, branchDiffObjectsFlag BranchDiffObjectsFlag, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	switch branchDiffObjectsFlag {
	case FromSourceToTarget, FromTargetToSource:
		// get source schema from source mysql
		sourceSchema, err := bs.sourceMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get target schema from target mysql
		targetSchema, err := bs.targetMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == FromSourceToTarget {
			return getBranchSchemaDiff(sourceSchema, targetSchema, hints)
		}
		return getBranchSchemaDiff(targetSchema, sourceSchema, hints)

	case FromTargetToSnapshot, FromSnapshotToTarget:
		// get target schema from target mysql
		targetSchema, err := bs.targetMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get snapshot schema that already saved in target mysql
		snapshotSchema, err := bs.targetMySQLService.getSnapshot(name)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == FromTargetToSnapshot {
			return getBranchSchemaDiff(targetSchema, snapshotSchema, hints)
		}
		return getBranchSchemaDiff(snapshotSchema, targetSchema, hints)

	case FromSnapshotToSource, FromSourceToSnapshot:
		// get source schema from source mysql
		sourceSchema, err := bs.sourceMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get snapshot schema that already saved in target mysql
		snapshotSchema, err := bs.targetMySQLService.getSnapshot(name)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == FromSnapshotToSource {
			return getBranchSchemaDiff(snapshotSchema, sourceSchema, hints)
		}
		return getBranchSchemaDiff(sourceSchema, snapshotSchema, hints)

	default:
		return nil, fmt.Errorf("%v is invalid branch diff objects flag, should be one of %v, %v, %v, %v, %v, %v",
			branchDiffObjectsFlag,
			FromSourceToTarget, FromTargetToSource,
			FromTargetToSnapshot, FromSnapshotToTarget,
			FromSnapshotToSource, FromSourceToSnapshot)
	}
}

// 根据option，计算schema
// 不对输入的meta做参数校验，默认输入为正确情况
// 幂等性：每次执行时都删除旧的ddl条目，重新计算并插入。
// 只有状态为preparing，prepared,merged，created时才能执行
// override: 试图将target分支覆盖掉source分支，
// merge：试图将target相当于snapshot的修改合并到source分支，会通过三路合并算法进行合并的冲突检测，若冲突则返回错误
// todo enhancement: schema diff hints
func (bs *BranchService) BranchPrepareMergeBack(name string, status BranchStatus, includeDatabases, excludeDatabases []string, mergeOption MergeBackOption) error {
	if mergeOption != MergeOverride && mergeOption != MergeDiff {
		return fmt.Errorf("%v is invalid merge option, should be one of %v or %v", mergeOption, MergeOverride, MergeDiff)
	}

	if !statusIsOneOf(status, []BranchStatus{StatusCreated, StatusPreparing, StatusPrepared, StatusMerged}) {
		return fmt.Errorf("%v is invalid status, should be one of %v or %v or %v or %v",
			status, StatusCreated, StatusPreparing, StatusPrepared, StatusMerged)
	}

	// set status to preparing
	err := bs.targetMySQLService.UpdateBranchStatus(name, StatusPreparing)
	if err != nil {
		return err
	}

	// delete all existing ddl entries in target database
	err = bs.targetMySQLService.deleteMergeBackDDL(name)
	if err != nil {
		return err
	}

	// calculate ddl based on merge option
	ddls := &BranchDiff{}
	hints := &schemadiff.DiffHints{}
	if mergeOption == MergeOverride {
		ddls, err = bs.getMergeBackOverrideDDLs(name, includeDatabases, excludeDatabases, hints)
		if err != nil {
			return err
		}
	} else if mergeOption == MergeDiff {
		ddls, err = bs.getMergeBackMergeDiffDDLs(name, includeDatabases, excludeDatabases, hints)
		if err != nil {
			return err
		}
	}
	// insert ddl into target database
	err = bs.targetMySQLService.insertMergeBackDDLInBatches(name, ddls, InsertMergeBackDDLBatchSize)
	if err != nil {
		return err
	}

	// set status to prepared
	return bs.targetMySQLService.UpdateBranchStatus(name, StatusPrepared)
}

// todo make it Idempotence
// todo enhancement: track whether the current ddl to apply has finished or is executing
// 幂等性：确保执行prepare merge中记录的ddl，crash后再次执行时，从上次没有执行的DDL继续。
// 难点：crash时，发送的那条DDL到底执行与否。有办法解决。但先记为todo，因为不同mysql协议数据库的解决方案不同。
// merge完成后，更新snapshot。
func (bs *BranchService) BranchMergeBack(name string, status BranchStatus) error {

	// 状态检查，只有prepared或者Merging才能执行
	if !statusIsOneOf(status, []BranchStatus{StatusPrepared, StatusMerging}) {
		return fmt.Errorf("%v is invalid status, should be one of %v or %v", status, StatusPrepared, StatusMerging)
	}

	// 将status改为Merging
	err := bs.targetMySQLService.UpdateBranchStatus(name, StatusMerging)
	if err != nil {
		return err
	}

	// 逐一获取，执行，记录ddl执行
	err = bs.executeMergeBackDDLOneByOne(name)
	if err != nil {
		return err
	}

	// 执行完毕后更新状态
	return bs.targetMySQLService.UpdateBranchStatus(name, StatusMerged)
	// todo 更新snapshot add
	// todo enhancement multi version snapshot

}

func (bs *BranchService) BranchShow(flag string) {
	// todo
	// use flag to decide what to show
	// meta status
	// snapshot
	// merge back ddl
}

/**********************************************************************************************************************/

func statusIsOneOf(status BranchStatus, statuses []BranchStatus) bool {
	for _, s := range statuses {
		if status == s {
			return true
		}
	}
	return false
}

func (bs *BranchService) executeMergeBackDDLOneByOne(name string) error {
	selectMergeBackDDLSQL := getSelectUnmergedDDLSQL(name)

	rows, err := bs.targetMySQLService.mysqlService.Query(selectMergeBackDDLSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id       int
			name     string
			database string
			table    string
			ddl      string
		)

		if err := rows.Scan(&id, &name, &database, &table, &ddl); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}
		// todo enhancement: track whether the current ddl to apply has finished or is executing
		_, err = bs.sourceMySQLService.mysqlService.Exec(ddl)
		if err != nil {
			return fmt.Errorf("failed to execute ddl: %v", err)
		}
		updateDDLMergedSQL := getUpdateDDLMergedSQL(id)
		_, err = bs.targetMySQLService.mysqlService.Exec(updateDDLMergedSQL)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *BranchService) getMergeBackOverrideDDLs(name string, includeDatabases, excludeDatabases []string, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	return bs.BranchDiff(name, includeDatabases, excludeDatabases, FromSourceToTarget, hints)
}

// todo complete me
func (bs *BranchService) getMergeBackMergeDiffDDLs(name string, includeDatabases, excludeDatabases []string, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	sourceSchema, err := bs.sourceMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
	if err != nil {
		return nil, err
	}
	targetSchema, err := bs.targetMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
	if err != nil {
		return nil, err
	}
	snapshot, err := bs.targetMySQLService.getSnapshot(name)
	if err != nil {
		return nil, err
	}

	conflict, message, err := branchSchemasConflictCheck(sourceSchema, targetSchema, snapshot, hints)
	if err != nil {
		return nil, err
	}
	if conflict {
		return nil, fmt.Errorf("branch schemas conflict: %v", message)
	}

	return getBranchSchemaDiff(snapshot, targetSchema, hints)
}

// this function check if the schema1 and schema2 (which are both derived from the snapshot schema) conflict using "three-way merge algorithm",
// the algorithm is described as follows:
//  1. Calculate the difference between snapshot and schema1 to get diff1,
//     apply diff1 to snapshot will get schema1 (which we represent as: diff1(snapshot) => schema1)
//  2. Get diff2 using the same way, diff2(snapshot) => schema2
//  3. Apply diff1 on diff2(snapshot), i.e., diff1(diff2(snapshot)). If it's invalid, there's a conflict.
//  4. Apply diff2 on diff1(snapshot), i.e., diff2(diff1(snapshot)). If it's invalid, there's a conflict.
//  5. If both are valid but diff1(diff2(snapshot)) is not equal to diff2(diff1(snapshot)), there's a conflict.
//  6. If both transformations are valid, and diff1(diff2(snapshot)) equals diff2(diff1(snapshot)), it signifies there's no conflict.
//
// 函数的输入是branchSchema，有多个数据库的schema
// todo add UT
// todo 注释
func branchSchemasConflictCheck(branchSchema1, branchSchema2, snapshot *BranchSchema, hints *schemadiff.DiffHints) (bool, string, error) {
	branchDiffFromSnapshotToSchema1, err := getBranchSchemaDiff(snapshot, branchSchema1, hints)
	if err != nil {
		return false, "", err
	}
	branchDiffFromSnapshotToSchema2, err := getBranchSchemaDiff(snapshot, branchSchema2, hints)
	if err != nil {
		return false, "", err
	}
	DatabaseSchemas1, err := branchSchemaToDatabaseSchemas(branchSchema1)
	if err != nil {
		return false, "", err
	}
	DatabaseSchemas2, err := branchSchemaToDatabaseSchemas(branchSchema2)
	if err != nil {
		return false, "", err
	}

	// diff1 应用到 bschema2上：
	NewDatabaseSchemas2, err := applyBranchDiffToDatabaseSchemas(DatabaseSchemas2, branchDiffFromSnapshotToSchema1)
	if err != nil {
		return false, "", err
	}
	// 同理将diff2应用到bschema1
	NewDatabaseSchemas1, err := applyBranchDiffToDatabaseSchemas(DatabaseSchemas1, branchDiffFromSnapshotToSchema2)
	if err != nil {
		return false, "", err
	}
	// 比较两个branch schema中的每一个schema是否都相同
	equal, message, err := databaseSchemasEqual(NewDatabaseSchemas1, NewDatabaseSchemas2, hints)
	if err != nil {
		return false, "", err
	}
	if !equal {
		// not equal, there's a conflict
		return true, message, nil
	}
	return false, "", nil
}

func branchSchemaToDatabaseSchemas(branchSchema *BranchSchema) (map[string]*schemadiff.Schema, error) {
	databaseSchemas := make(map[string]*schemadiff.Schema)
	for dbName, databaseSchemaInMap := range branchSchema.branchSchema {
		tableSchemaQueries := make([]string, 0)
		for _, tableSchemaQuery := range databaseSchemaInMap {
			tableSchemaQueries = append(tableSchemaQueries, tableSchemaQuery)
		}

		databaseSchema, err := schemadiff.NewSchemaFromQueries(tableSchemaQueries)
		if err != nil {
			return nil, err
		}
		databaseSchemas[dbName] = databaseSchema
	}
	return databaseSchemas, nil
}

// todo comment
// todo add UT
func applyBranchDiffToDatabaseSchemas(databaseSchemas map[string]*schemadiff.Schema, branchDiff *BranchDiff) (map[string]*schemadiff.Schema, error) {
	resultDatabaseSchemas := make(map[string]*schemadiff.Schema)
	for database, databaseDiff := range branchDiff.diffs {
		// 处理删除数据库的情况
		if databaseDiff.needDropDatabase {
			if _, exists := databaseSchemas[database]; !exists {
				return nil, fmt.Errorf("database %s not exist in databaseSchemas, can not drop database", database)
			}
			// this database will not occur in the result, skip
			continue
		}

		// 处理创建一个新数据库的情况
		if databaseDiff.needCreateDatabase {
			if _, exists := databaseSchemas[database]; exists {
				return nil, fmt.Errorf("database %s already exist in databaseSchemas, can not create database", database)
			}

			createTableQueries := make([]string, 0)
			for _, createTableQuery := range databaseDiff.tableDDLs {
				// 虽然这里是数组，但是由于每张表都不存在，因此实际上数组只有一个元素，且都是create table语句
				createTableQueries = append(createTableQueries, createTableQuery...)
			}
			databaseSchema, err := schemadiff.NewSchemaFromQueries(createTableQueries)
			if err != nil {
				return nil, err
			}
			resultDatabaseSchemas[database] = databaseSchema
			continue
		}

		// 处理已有数据库的情况
		if _, exists := databaseSchemas[database]; !exists {
			return nil, fmt.Errorf("database %s not exist in databaseSchemas, can not modify database", database)
		}

		// 将该数据库的diff应用到
		databaseSchema := databaseSchemas[database]
		entityDiffs := make([]schemadiff.EntityDiff, 0)
		for _, entityDiff := range databaseDiff.tableEntityDiffs {
			entityDiffs = append(entityDiffs, entityDiff)
		}
		newDatabaseSchema, err := databaseSchema.Apply(entityDiffs)
		if err != nil {
			return nil, err
		}
		resultDatabaseSchemas[database] = newDatabaseSchema
	}
	return resultDatabaseSchemas, nil
}

// todo UT
func databaseSchemasEqual(databaseSchemas1, databaseSchemas2 map[string]*schemadiff.Schema, hints *schemadiff.DiffHints) (bool, string, error) {
	if len(databaseSchemas1) != len(databaseSchemas2) {
		return false, fmt.Sprintf("number of databases not equal: %d != %d",
			len(databaseSchemas1), len(databaseSchemas2)), nil
	}

	for dbName, schema1 := range databaseSchemas1 {
		schema2, exists := databaseSchemas2[dbName]
		if !exists {
			return false, fmt.Sprintf("database %s exists in source but not in target", dbName), nil
		}

		equal, message, err := databaseSchemaEqual(schema1, schema2, hints)
		if err != nil {
			return false, "", err
		}
		if !equal {
			return false, fmt.Sprintf("database %s: %s", dbName, message), nil
		}
	}

	return true, "", nil
}

// todo add UT
func databaseSchemaEqual(databaseSchema1, databaseSchema2 *schemadiff.Schema, hints *schemadiff.DiffHints) (bool, string, error) {
	tables1 := databaseSchema1.Tables()
	tables2 := databaseSchema2.Tables()

	if len(tables1) != len(tables2) {
		return false, fmt.Sprintf("number of tables not equal: %d != %d",
			len(tables1), len(tables2)), nil
	}

	tableMap1 := make(map[string]*schemadiff.CreateTableEntity)
	for _, table := range tables1 {
		tableMap1[table.Name()] = table
	}

	tableMap2 := make(map[string]*schemadiff.CreateTableEntity)
	for _, table := range tables2 {
		tableMap2[table.Name()] = table
	}

	for tableName, table1 := range tableMap1 {
		table2, exists := tableMap2[tableName]
		if !exists {
			return false, fmt.Sprintf("table %s exists in source but not in target", tableName), nil
		}

		equal, message, err := tableSchemaEqual(table1, table2, hints)
		if err != nil {
			return false, "", err
		}
		if !equal {
			return false, message, nil
		}
	}

	return true, "", nil
}

// todo add UT
func tableSchemaEqual(tableSchema1, tableSchema2 *schemadiff.CreateTableEntity, hints *schemadiff.DiffHints) (bool, string, error) {
	entityDiff, err := tableSchema1.Diff(tableSchema2, hints)
	if err != nil {
		return false, "", err
	}
	_, ddls, err := schemadiff.GetDDLFromTableDiff(entityDiff, "", "")
	if err != nil {
		return false, "", err
	}
	if len(ddls) > 0 {
		ddlMessage := strings.Join(ddls, ";")
		message := fmt.Sprintf("table %v and %v are not equal, from %v to %v: %v", tableSchema1.Name(), tableSchema2.Name(), tableSchema1, tableSchema2, ddlMessage)
		return false, message, nil
	}
	return true, "", nil
}

// branchFetchSnapshot retrieves the schema from the source MySQL instance and stores it
// as a snapshot in the target instance.
//
// Idempotency:
// This function is idempotent by design:
// - It always removes all existing snapshot entries before inserting new ones
// - Each execution will result in a fresh snapshot state
//
// Parameters:
// - branchMeta: Contains the branch metadata and configuration
//
// Returns:
// - *BranchSchema: The fetched schema information
// - error: Returns nil on success, error otherwise
func (bs *BranchService) branchFetchSnapshot(name string, includeDatabases, excludeDatabases []string) (*BranchSchema, error) {
	// get schema from source
	schema, err := bs.sourceMySQLService.GetBranchSchema(includeDatabases, excludeDatabases)
	if err != nil {
		return nil, err
	}

	// delete all snapshot entries in target database
	err = bs.targetMySQLService.deleteSnapshot(name)
	if err != nil {
		return nil, err
	}

	// insert snapshot schema into target database
	err = bs.targetMySQLService.insertSnapshotInBatches(name, schema, InsertSnapshotBatchSize)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// todo enhancement: target database pattern
func getBranchSchemaDiff(originSchema *BranchSchema, expectSchema *BranchSchema, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	branchDiff := &BranchDiff{diffs: make(map[string]*DatabaseDiff)}

	// databases exist in originSchema but not exist in expectSchema
	for dbName := range originSchema.branchSchema {
		if _, exist := expectSchema.branchSchema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				needCreateDatabase: false,
				needDropDatabase:   true,
			}
			branchDiff.diffs[dbName] = databaseDiff
		}
	}

	// databases exist in expectSchema but not exist in originSchema
	for dbName := range expectSchema.branchSchema {
		if _, exist := originSchema.branchSchema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				needCreateDatabase: true,
				needDropDatabase:   false,
			}
			tableDDLs := make(map[string][]string)
			tableEntityDiff := make(map[string]schemadiff.EntityDiff)
			// generate create table ddl for each tables
			for tableName, schema := range expectSchema.branchSchema[dbName] {
				tableDiffs := make([]string, 0)
				diff, err := schemadiff.DiffCreateTablesQueries("", schema, hints)
				if err != nil {
					return nil, err
				}

				_, ddls, err := schemadiff.GetDDLFromTableDiff(diff, dbName, tableName)
				if err != nil {
					return nil, err
				}
				tableDiffs = append(tableDiffs, ddls...)
				tableDDLs[tableName] = tableDiffs
				tableEntityDiff[tableName] = diff
			}
			databaseDiff.tableDDLs = tableDDLs
			databaseDiff.tableEntityDiffs = tableEntityDiff
			branchDiff.diffs[dbName] = databaseDiff
		}
	}

	// databases exist in both originSchema and expectSchema
	for dbName, expectTables := range expectSchema.branchSchema {
		originTables, exist := originSchema.branchSchema[dbName]
		if !exist {
			continue
		}
		databaseDiff := &DatabaseDiff{
			needCreateDatabase: false,
			needDropDatabase:   false,
		}
		tableDDLs := make(map[string][]string)
		tableEntityDiff := make(map[string]schemadiff.EntityDiff)

		// tables exist in originSchema but not exist in expectSchema
		for tableName, originSchema := range originTables {
			if _, exist := expectTables[tableName]; !exist {
				tableDiffs := make([]string, 0)
				diff, err := schemadiff.DiffCreateTablesQueries(originSchema, "", hints)
				if err != nil {
					return nil, err
				}
				_, ddls, err := schemadiff.GetDDLFromTableDiff(diff, dbName, tableName)
				if err != nil {
					return nil, err
				}
				tableDiffs = append(tableDiffs, ddls...)
				tableDDLs[tableName] = tableDiffs
				tableEntityDiff[tableName] = diff
			}
		}

		// tables exist in expectSchema but not exist in originSchema
		for tableName, expectSchema := range expectTables {
			if _, exist := originTables[tableName]; !exist {
				tableDiffs := make([]string, 0)
				diff, err := schemadiff.DiffCreateTablesQueries("", expectSchema, hints)
				if err != nil {
					return nil, err
				}
				_, ddls, err := schemadiff.GetDDLFromTableDiff(diff, dbName, tableName)
				if err != nil {
					return nil, err
				}
				tableDiffs = append(tableDiffs, ddls...)
				tableDDLs[tableName] = tableDiffs
				tableEntityDiff[tableName] = diff
			}
		}

		// tables exist in both originSchema and expectSchema
		for tableName, expectSchema := range expectTables {
			if originSchema, exist := originTables[tableName]; exist {
				tableDiffs := make([]string, 0)
				diff, err := schemadiff.DiffCreateTablesQueries(originSchema, expectSchema, hints)
				if err != nil {
					return nil, err
				}
				_, ddls, err := schemadiff.GetDDLFromTableDiff(diff, dbName, tableName)
				if err != nil {
					return nil, err
				}
				tableDiffs = append(tableDiffs, ddls...)
				tableDDLs[tableName] = tableDiffs
				tableEntityDiff[tableName] = diff
			}
		}
		databaseDiff.tableDDLs = tableDDLs
		databaseDiff.tableEntityDiffs = tableEntityDiff
		branchDiff.diffs[dbName] = databaseDiff
	}

	return branchDiff, nil
}

func addDefaultExcludeDatabases(branchMeta *BranchMeta) {
	for _, db := range DefaultExcludeDatabases {
		has := false
		for _, db2 := range branchMeta.excludeDatabases {
			if db == db2 {
				has = true
				break
			}
		}
		if !has {
			branchMeta.excludeDatabases = append(branchMeta.excludeDatabases, db)
		}
	}
}
