package branch

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/failpoint"
	"strings"
	"vitess.io/vitess/go/vt/failpointkey"
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
	includeDBs, excludeDBs string) (*BranchMeta, error) {

	var includeDatabases []string
	if includeDBs == "" {
		return nil, fmt.Errorf("IncludeDatabases cannot be empty")
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
			excludeDatabases[i] = db
		}
	}

	bMeta := &BranchMeta{
		Name:             name,
		SourceHost:       sourceHost,
		SourcePort:       sourcePort,
		SourceUser:       sourceUser,
		SourcePassword:   sourcePassword,
		IncludeDatabases: includeDatabases,
		ExcludeDatabases: excludeDatabases,
		Status:           StatusInit,
	}

	addDefaultExcludeDatabases(bMeta)

	err := bMeta.Validate()
	if err != nil {
		return nil, err
	}

	return bMeta, nil
}

func (meta *BranchMeta) Validate() error {
	if meta.Name == "" {
		return fmt.Errorf("branch Name cannot be empty")
	}

	if meta.SourceHost == "" {
		return fmt.Errorf("branch SourceHost cannot be empty")
	}

	if meta.SourcePort <= 0 || meta.SourcePort > 65535 {
		return fmt.Errorf("branch invalid SourcePort: %d", meta.SourcePort)
	}

	if meta.IncludeDatabases == nil || len(meta.IncludeDatabases) == 0 {
		return fmt.Errorf("branch IncludeDatabases cannot be empty")
	}
	for _, db := range meta.ExcludeDatabases {
		if db == "*" {
			return fmt.Errorf("ExcludeDatabases contains wildcard '*', branching is meaningless")
		}
	}

	switch meta.Status {
	case StatusInit, StatusFetched, StatusCreated, StatusPreparing, StatusPrepared, StatusMerging, StatusMerged:
	default:
		return fmt.Errorf("branch invalid Status: %s", meta.Status)
	}

	return nil
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
	if branchMeta.Status != StatusInit {
		return fmt.Errorf("the Status of branch meta should be init")
	}
	meta, err := bs.targetMySQLService.SelectOrInsertBranchMeta(branchMeta)
	if err != nil {
		return err
	}
	if meta.Status == StatusInit || meta.Status == StatusUnknown {
		_, err := bs.branchFetchSnapshot(meta.Name, meta.IncludeDatabases, meta.ExcludeDatabases)
		if err != nil {
			return err
		}
		// upsert make sure the meta stored in mysql will be synced
		meta.Status = StatusFetched
		err = bs.targetMySQLService.UpsertBranchMeta(meta)
		if err != nil {
			return err
		}
	}

	if meta.Status == StatusFetched {
		err := bs.targetMySQLService.ApplySnapshot(meta.Name)
		if err != nil {
			return err
		}
		meta.Status = StatusCreated
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

// BranchPrepareMergeBack prepares target branch for merging changes back into source branch based on the specified merge option.
//
// - The function ensures idempotency by deleting existing DDL entries before recalculating and inserting new ones.
// - This function can only be executed if the branch status is "preparing", "prepared", "merged", or "created".
//
// Parameters:
// - name: The name of the branch.
// - status: The current status of the branch. It must be one of "preparing", "prepared", "merged", or "created".
// - includeDatabases: A list of databases to include in the operation.
// - excludeDatabases: A list of databases to exclude from the operation.
// - mergeOption: Determines the merge strategy. Possible values:
//   - MergeOverride: Attempts to override the source branch with the target branch.
//   - MergeDiff: Attempts to merge the modifications of the target branch relative to a snapshot into the source branch. Uses a three-way merge algorithm to detect conflicts. Returns an error if conflicts are detected.
//
// - hints: Optional hints to guide the schema difference calculation process.
// Returns:
// - BranchDiff: The calculated DDL operations required for the merge-back.
// - error: An error if any step of the process fails.
func (bs *BranchService) BranchPrepareMergeBack(name string, status BranchStatus, includeDatabases, excludeDatabases []string, mergeOption MergeBackOption, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	if mergeOption != MergeOverride && mergeOption != MergeDiff {
		return nil, fmt.Errorf("%v is invalid merge option, should be one of %v or %v", mergeOption, MergeOverride, MergeDiff)
	}

	if !statusIsOneOf(status, []BranchStatus{StatusCreated, StatusPreparing, StatusPrepared, StatusMerged}) {
		return nil, fmt.Errorf("%v is invalid Status, should be one of %v or %v or %v or %v",
			status, StatusCreated, StatusPreparing, StatusPrepared, StatusMerged)
	}

	// set Status to preparing
	err := bs.targetMySQLService.UpdateBranchStatus(name, StatusPreparing)
	if err != nil {
		return nil, err
	}

	// delete all existing ddl entries in target database
	err = bs.targetMySQLService.deleteMergeBackDDL(name)
	if err != nil {
		return nil, err
	}

	// calculate ddl based on merge option
	ddls := &BranchDiff{}
	if mergeOption == MergeOverride {
		ddls, err = bs.getMergeBackOverrideDDLs(name, includeDatabases, excludeDatabases, hints)
		if err != nil {
			return nil, err
		}
	} else if mergeOption == MergeDiff {
		ddls, err = bs.getMergeBackMergeDiffDDLs(name, includeDatabases, excludeDatabases, hints)
		if err != nil {
			return nil, err
		}
	}
	// insert ddl into target database
	err = bs.targetMySQLService.insertMergeBackDDLInBatches(name, ddls, InsertMergeBackDDLBatchSize)
	if err != nil {
		return nil, err
	}

	// set Status to prepared
	err = bs.targetMySQLService.UpdateBranchStatus(name, StatusPrepared)
	if err != nil {
		return nil, err
	}

	return ddls, nil
}

// BranchMergeBack executes the prepared DDLs for merging target branch back into source branch in an idempotent manner.
// If the operation crashes or is interrupted, it ensures that subsequent executions will continue from the last uncompleted DDL.
//
// Parameters:
// - name: The name of the target branch for the merge-back operation.
// - status: The current status of the branch. It must be one of the following:
//   - StatusPrepared: Indicates that the branch has been prepared for merging.
//   - StatusMerging: Indicates that the merge operation is already in progress.
//
// Returns:
// - error: An error if any step of the process fails.
//
// Notes:
// - The function is designed to ensure idempotency, meaning it can handle interruptions and crashes gracefully.
// - Enhancements planned for the future include:
//   - Tracking whether the current DDL being applied has finished or is still executing.
//   - Updating the snapshot to allow merging more than once.
//   - Supporting multi-version snapshots.
func (bs *BranchService) BranchMergeBack(name string, status BranchStatus) error {
	// check status
	if !statusIsOneOf(status, []BranchStatus{StatusPrepared, StatusMerging}) {
		return fmt.Errorf("%v is invalid Status, should be one of %v or %v", status, StatusPrepared, StatusMerging)
	}

	// set status Merging
	err := bs.targetMySQLService.UpdateBranchStatus(name, StatusMerging)
	if err != nil {
		return err
	}

	// select and execute merge back ddl one by one
	// todo enhancement: track whether the current ddl to apply has finished or is executing
	err = bs.executeMergeBackDDL(name)
	if err != nil {
		return err
	}

	// set status Merged
	return bs.targetMySQLService.UpdateBranchStatus(name, StatusMerged)

	// todo enhancement: update snapshot to support merging more than once
	// todo enhancement: multi version snapshot

}

func (t *TargetMySQLService) BranchCleanUp(name string) error {
	deleteMeta := getDeleteBranchMetaSQL(name)
	deleteSnapshot := getDeleteSnapshotSQL(name)
	deleteMergeBackDDL := getDeleteMergeBackDDLSQL(name)
	return t.mysqlService.ExecuteInTxn(deleteMeta, deleteSnapshot, deleteMergeBackDDL)
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

func (bs *BranchService) executeMergeBackDDL(name string) error {
	// create or drop database first
	selectUnmergedDBDDLSQL := getSelectUnmergedDBDDLSQL(name)
	rows, err := bs.targetMySQLService.mysqlService.Query(selectUnmergedDBDDLSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = bs.executeMergeBackDDLOneByOne(rows)
	if err != nil {
		return err
	}

	// then, execute table ddl
	selectMergeBackDDLSQL := getSelectUnmergedDDLSQL(name)

	rows2, err := bs.targetMySQLService.mysqlService.Query(selectMergeBackDDLSQL)
	if err != nil {
		return err
	}
	defer rows2.Close()
	return bs.executeMergeBackDDLOneByOne(rows2)
}

// caller should close rows
func (bs *BranchService) executeMergeBackDDLOneByOne(rows *sql.Rows) error {
	for rows.Next() {
		var (
			id       int
			name     string
			database string
			table    string
			ddl      string
			merged   bool
		)
		var err error
		if err = rows.Scan(&id, &name, &database, &table, &ddl, &merged); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		if table == "" {
			// create or drop database ddl
			_, err = bs.sourceMySQLService.mysqlService.Exec("", ddl)
		} else {
			// todo enhancement: track whether the current ddl to apply has finished or is executing
			_, err = bs.sourceMySQLService.mysqlService.Exec(database, ddl)
		}
		if err != nil {
			return fmt.Errorf("failed to execute ddl %v: %v", ddl, err)
		}
		updateDDLMergedSQL := getUpdateDDLMergedSQL(id)
		_, err = bs.targetMySQLService.mysqlService.Exec("", updateDDLMergedSQL)
		if err != nil {
			return err
		}
		failpoint.Inject(failpointkey.BranchExecuteMergeBackDDLError.Name, func() {
			failpoint.Return(fmt.Errorf("error executing merge back ddl by failpoint"))
		})
	}

	return nil
}

func (bs *BranchService) getMergeBackOverrideDDLs(name string, includeDatabases, excludeDatabases []string, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	return bs.BranchDiff(name, includeDatabases, excludeDatabases, FromSourceToTarget, hints)
}

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
// Notes: The parameters are BranchSchema Type, which concludes the multi database schema, it's an instance-level schema.
// todo add UT
func branchSchemasConflictCheck(branchSchema1, branchSchema2, snapshot *BranchSchema, hints *schemadiff.DiffHints) (bool, string, error) {
	branchDiffFromSnapshotToSchema1, err := getBranchSchemaDiff(snapshot, branchSchema1, hints)
	if err != nil {
		return false, "", err
	}
	branchDiffFromSnapshotToSchema2, err := getBranchSchemaDiff(snapshot, branchSchema2, hints)
	if err != nil {
		return false, "", err
	}
	instanceSchema1, err := branchSchemaToInstanceSchema(branchSchema1)
	if err != nil {
		return false, "", err
	}
	instanceSchema2, err := branchSchemaToInstanceSchema(branchSchema2)
	if err != nil {
		return false, "", err
	}

	// apply diff1 to schema2
	NewInstanceSchema2, err := applyBranchDiffToInstanceSchema(instanceSchema2, branchDiffFromSnapshotToSchema1)
	if err != nil {
		return false, "", err
	}
	// apply diff2 to schema1
	NewInstanceSchema1, err := applyBranchDiffToInstanceSchema(instanceSchema1, branchDiffFromSnapshotToSchema2)
	if err != nil {
		return false, "", err
	}
	// compare two instance-level schemas
	equal, message, err := instanceSchemaEqual(NewInstanceSchema1, NewInstanceSchema2, hints)
	if err != nil {
		return false, "", err
	}
	if !equal {
		// not equal, there's a conflict
		return true, message, nil
	}
	return false, "", nil
}

func branchSchemaToInstanceSchema(branchSchema *BranchSchema) (map[string]*schemadiff.Schema, error) {
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

// todo add UT
func applyBranchDiffToInstanceSchema(instanceSchema map[string]*schemadiff.Schema, branchDiff *BranchDiff) (map[string]*schemadiff.Schema, error) {
	resultDatabaseSchemas := make(map[string]*schemadiff.Schema)
	for database, databaseDiff := range branchDiff.Diffs {
		// need drop database
		if databaseDiff.NeedDropDatabase {
			if _, exists := instanceSchema[database]; !exists {
				return nil, fmt.Errorf("database %s not exist in instanceSchema, can not drop database", database)
			}
			// this database will not occur in the result, skip
			continue
		}

		// need create new database
		if databaseDiff.NeedCreateDatabase {
			if _, exists := instanceSchema[database]; exists {
				return nil, fmt.Errorf("database %s already exist in instanceSchema, can not create database", database)
			}

			createTableQueries := make([]string, 0)
			for _, createTableQuery := range databaseDiff.TableDDLs {
				// Although this appears to be an array, since each table doesn't exist,
				// the array actually only contains one element, which is a CREATE TABLE statement.
				createTableQueries = append(createTableQueries, createTableQuery...)
			}
			databaseSchema, err := schemadiff.NewSchemaFromQueries(createTableQueries)
			if err != nil {
				return nil, err
			}
			resultDatabaseSchemas[database] = databaseSchema
			continue
		}

		// deal with existing database
		if _, exists := instanceSchema[database]; !exists {
			return nil, fmt.Errorf("database %s not exist in instanceSchema, can not modify database", database)
		}

		// apply diff to database
		databaseSchema := instanceSchema[database]
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
func instanceSchemaEqual(instanceSchema1, instanceSchema2 map[string]*schemadiff.Schema, hints *schemadiff.DiffHints) (bool, string, error) {
	if len(instanceSchema1) != len(instanceSchema2) {
		return false, fmt.Sprintf("number of databases not equal: %d != %d",
			len(instanceSchema1), len(instanceSchema2)), nil
	}

	for dbName, schema1 := range instanceSchema1 {
		schema2, exists := instanceSchema2[dbName]
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
// todo enhancement: deal with charset
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
		message := fmt.Sprintf("table %v and %v are not equal, from %v to %v: %v", tableSchema1.Name(), tableSchema2.Name(), tableSchema1.Name(), tableSchema2.Name(), ddlMessage)
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
	failpoint.Inject(failpointkey.BranchFetchSnapshotError.Name, func() {
		failpoint.Return(nil, fmt.Errorf("error fetching snapshot by failpoint"))
	})
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
	branchDiff := &BranchDiff{Diffs: make(map[string]*DatabaseDiff)}

	// databases exist in originSchema but not exist in expectSchema
	for dbName := range originSchema.branchSchema {
		if _, exist := expectSchema.branchSchema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				NeedCreateDatabase: false,
				NeedDropDatabase:   true,
			}
			branchDiff.Diffs[dbName] = databaseDiff
		}
	}

	// databases exist in expectSchema but not exist in originSchema
	for dbName := range expectSchema.branchSchema {
		if _, exist := originSchema.branchSchema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				NeedCreateDatabase: true,
				NeedDropDatabase:   false,
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
			databaseDiff.TableDDLs = tableDDLs
			databaseDiff.tableEntityDiffs = tableEntityDiff
			branchDiff.Diffs[dbName] = databaseDiff
		}
	}

	// databases exist in both originSchema and expectSchema
	for dbName, expectTables := range expectSchema.branchSchema {
		originTables, exist := originSchema.branchSchema[dbName]
		if !exist {
			continue
		}
		databaseDiff := &DatabaseDiff{
			NeedCreateDatabase: false,
			NeedDropDatabase:   false,
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
		databaseDiff.TableDDLs = tableDDLs
		databaseDiff.tableEntityDiffs = tableEntityDiff
		branchDiff.Diffs[dbName] = databaseDiff
	}

	return branchDiff, nil
}

func addDefaultExcludeDatabases(branchMeta *BranchMeta) {
	for _, db := range DefaultExcludeDatabases {
		has := false
		for _, db2 := range branchMeta.ExcludeDatabases {
			if db == db2 {
				has = true
				break
			}
		}
		if !has {
			branchMeta.ExcludeDatabases = append(branchMeta.ExcludeDatabases, db)
		}
	}
}
