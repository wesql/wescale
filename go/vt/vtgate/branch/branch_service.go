package branch

import (
	"fmt"
	"strings"
	"vitess.io/vitess/go/vt/schemadiff"
)

var (
	DefaultExcludeDatabases = []string{"mysql", "sys", "information_schema", "performance_schema"}

	BranchDiffObjectsSourceTarget   = "source_target" // which means wants diff from source schema to target schema
	BranchDiffObjectsTargetSource   = "target_source"
	BranchDiffObjectsSourceSnapshot = "source_snapshot"
	BranchDiffObjectsSnapshotSource = "snapshot_source"
	BranchDiffObjectsTargetSnapshot = "target_snapshot"
	BranchDiffObjectsSnapshotTarget = "snapshot_target"

	MergeOptionOverride = "override"
	MergeOptionMerge    = "merge"
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
func (bs *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	if branchMeta.status != StatusInit {
		return fmt.Errorf("the status of branch meta should be init")
	}
	meta, err := bs.targetMySQLService.SelectOrInsertBranchMeta(branchMeta)
	if err != nil {
		return err
	}
	if meta.status == StatusInit || meta.status == StatusUnknown {
		_, err := bs.branchFetchSnapshot(meta)
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
		err := bs.targetMySQLService.ApplySnapshot(meta)
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
// - SourceTarget/TargetSource: Compares source MySQL schema with target MySQL schema
// - TargetSnapshot/SnapshotTarget: Compares target MySQL schema with stored snapshot
// - SnapshotSource/SourceSnapshot: Compares stored snapshot with source MySQL schema
//
// Parameters:
// - branchMeta: Contains branch configuration including database filters
// - branchDiffObjectsFlag: Specifies which objects to compare and comparison direction
// - hints: Additional configuration for diff calculation
//
// Returns:
// - *BranchDiff: Contains the calculated schema differences
// - error: Returns nil on success, error on invalid flag or retrieval failure
func (bs *BranchService) BranchDiff(branchMeta *BranchMeta, branchDiffObjectsFlag string, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	switch branchDiffObjectsFlag {
	case BranchDiffObjectsSourceTarget, BranchDiffObjectsTargetSource:
		// get source schema from source mysql
		sourceSchema, err := bs.sourceMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get target schema from target mysql
		targetSchema, err := bs.targetMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == BranchDiffObjectsSourceTarget {
			return getBranchDiff(sourceSchema, targetSchema, hints)
		}
		return getBranchDiff(targetSchema, sourceSchema, hints)

	case BranchDiffObjectsTargetSnapshot, BranchDiffObjectsSnapshotTarget:
		// get target schema from target mysql
		targetSchema, err := bs.targetMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get snapshot schema that already saved in target mysql
		snapshotSchema, err := bs.targetMySQLService.getSnapshot(branchMeta)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == BranchDiffObjectsTargetSnapshot {
			return getBranchDiff(targetSchema, snapshotSchema, hints)
		}
		return getBranchDiff(snapshotSchema, targetSchema, hints)

	case BranchDiffObjectsSnapshotSource, BranchDiffObjectsSourceSnapshot:
		// get source schema from source mysql
		sourceSchema, err := bs.sourceMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
		if err != nil {
			return nil, err
		}
		// get snapshot schema that already saved in target mysql
		snapshotSchema, err := bs.targetMySQLService.getSnapshot(branchMeta)
		if err != nil {
			return nil, err
		}
		// return diff
		if branchDiffObjectsFlag == BranchDiffObjectsSnapshotSource {
			return getBranchDiff(snapshotSchema, sourceSchema, hints)
		}
		return getBranchDiff(sourceSchema, snapshotSchema, hints)

	default:
		return nil, fmt.Errorf("%v is invalid branch diff objects flag, should be one of %v, %v, %v, %v, %v, %v",
			branchDiffObjectsFlag,
			BranchDiffObjectsSourceTarget, BranchDiffObjectsTargetSource,
			BranchDiffObjectsTargetSnapshot, BranchDiffObjectsSnapshotTarget,
			BranchDiffObjectsSnapshotSource, BranchDiffObjectsSourceSnapshot)
	}
}

// 根据option，计算schema
// 不对输入的meta做参数校验，默认输入为正确情况
// 幂等性：每次执行时都删除旧的ddl条目，重新计算并插入。
// 只有状态为preparing，prepared,merged，created时才能执行
// override: 试图将target分支覆盖掉source分支，
// merge：试图将target相当于snapshot的修改合并到source分支，会通过三路合并算法进行合并的冲突检测，若冲突则返回错误
// todo complete me
func (bs *BranchService) BranchPrepareMerge(meta *BranchMeta, mergeOption string) error {
	if mergeOption != MergeOptionOverride && mergeOption != MergeOptionMerge {
		return fmt.Errorf("%v is invalid merge option, should be one of %v or %v", mergeOption, MergeOptionOverride, MergeOptionMerge)
	}

	if meta.status != StatusCreated && meta.status != StatusPreparing && meta.status != StatusPrepared && meta.status != StatusMerged {
		return fmt.Errorf("%v is invalid status, should be one of %v or %v or %v or %v",
			meta.status, StatusCreated, StatusPreparing, StatusPrepared, StatusMerged)
	}

	// set status to preparing
	meta.status = StatusPreparing
	err := bs.targetMySQLService.UpsertBranchMeta(meta)
	if err != nil {
		return err
	}

	// delete all existing ddl entries in target database
	err = bs.targetMySQLService.deleteSnapshot(meta)
	if err != nil {
		return err
	}

	// calculate ddl based on merge option

	// insert ddl into target database
	// set status to prepared

	return nil
}

// todo make it Idempotence
// 幂等性：确保执行prepare merge中记录的ddl，crash后再次执行时，从上次没有执行的DDL继续。
// 难点：crash时，发送的那条DDL到底执行与否。有办法解决。但先记为todo，因为不同mysql协议数据库的解决方案不同。
// merge完成后，更新snapshot。
func (bs *BranchService) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (bs *BranchService) BranchShow() {
	// todo
	// use flag to decide what to show
}

/**********************************************************************************************************************/

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
func (bs *BranchService) branchFetchSnapshot(branchMeta *BranchMeta) (*BranchSchema, error) {
	// get schema from source
	schema, err := bs.sourceMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
	if err != nil {
		return nil, err
	}

	// delete all snapshot entries in target database
	err = bs.targetMySQLService.deleteSnapshot(branchMeta)
	if err != nil {
		return nil, err
	}

	// insert snapshot schema into target database
	err = bs.targetMySQLService.insertSnapshotInBatches(branchMeta, schema, InsertSnapshotBatchSize)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// todo enhancement: target database pattern
func getBranchDiff(originSchema *BranchSchema, expectSchema *BranchSchema, hints *schemadiff.DiffHints) (*BranchDiff, error) {
	branchDiff := &BranchDiff{diffs: make(map[string]*DatabaseDiff)}

	// databases exist in originSchema but not exist in expectSchema
	for dbName := range originSchema.schema {
		if _, exist := expectSchema.schema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				needCreate: false,
				needDrop:   true,
			}
			branchDiff.diffs[dbName] = databaseDiff
		}
	}

	// databases exist in expectSchema but not exist in originSchema
	for dbName := range expectSchema.schema {
		if _, exist := originSchema.schema[dbName]; !exist {
			databaseDiff := &DatabaseDiff{
				needCreate: true,
				needDrop:   false,
			}
			tableDDLs := make(map[string][]string)
			// generate create table ddl for each tables
			for tableName, schema := range expectSchema.schema[dbName] {
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
			}
			databaseDiff.tableDDLs = tableDDLs
			branchDiff.diffs[dbName] = databaseDiff
		}
	}

	// databases exist in both originSchema and expectSchema
	for dbName, expectTables := range expectSchema.schema {
		originTables, exist := originSchema.schema[dbName]
		if !exist {
			continue
		}
		databaseDiff := &DatabaseDiff{
			needCreate: false,
			needDrop:   false,
		}
		tableDDLs := make(map[string][]string)

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
			}
		}
		databaseDiff.tableDDLs = tableDDLs
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
