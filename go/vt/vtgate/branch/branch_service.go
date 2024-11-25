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

// todo optimize think of failure handling
// 幂等性 ：如果没创建，则创建，否则不管。
// 创建崩溃后，再次运行这个命令即可。
// 如果状态是已经创建了，那么就不需要再走继续的流程的，返回成功
// 如果是其他的状态，也会直接返回。
func (bs *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	if branchMeta.status != StatusInit {
		return fmt.Errorf("the status of branch meta should be init")
	}
	meta, err := bs.targetMySQLService.SelectOrInsertBranchMeta(branchMeta)
	if err != nil {
		return err
	}
	if meta.status == StatusInit || meta.status == StatusUnknown {
		_, err := bs.branchFetch(meta)
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
		meta.status = statusCreated
		return bs.targetMySQLService.UpsertBranchMeta(meta)
	}

	return nil
}

// 根据传入的参数，选择要产生diff的schema，然后计算diff，返回结果
// 确保调用时branch meta的正确性，特别是其中的inlcude/ exclude字段
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

func (bs *BranchService) BranchPrepareMerge(meta BranchMeta) {
	// todo
	// PrepareMerge
	// get schemas from source and target through mysql connection
	// calculate diffs based on merge options such as override or merge
}

func (bs *BranchService) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (bs *BranchService) BranchShow() {
	//todo
}

/**********************************************************************************************************************/

// todo make it Idempotence
// todo comment
// todo fix me
// 每次都删除所有条目，重新获取和插入。
// 幂等性：获取最新的source端的schema并存入target端，每次执行会删除之前的所有条目并重新查询。
func (bs *BranchService) branchFetch(branchMeta *BranchMeta) (*BranchSchema, error) {
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
