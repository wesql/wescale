package branch

import (
	"fmt"
	"strings"
	"vitess.io/vitess/go/vt/schemadiff"
)

var DefaultExcludeDatabases = []string{"mysql", "sys", "information_schema", "performance_schema"}

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
		status:           StatusInit, // 设置初始状态
	}

	addDefaultExcludeDatabases(bMeta)
	return bMeta, nil
}

// todo optimize think of failure handling
func (bs *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	// get schema from source and store to target
	branchSchema, err := bs.BranchFetch(branchMeta)
	if err != nil {
		return err
	}

	// stmts act as the WAL for CreateDatabaseAndTablesIfNotExists
	err = bs.targetMySQLService.CreateDatabaseAndTablesIfNotExists(branchSchema)
	if err != nil {
		return err
	}

	// todo optimize wait for tables created

	return nil
}

// todo refactor me
func (bs *BranchService) BranchFetch(branchMeta *BranchMeta) (*BranchSchema, error) {
	// Get all create table statements except system databases
	schema, err := bs.sourceMySQLService.GetBranchSchema(branchMeta.includeDatabases, branchMeta.excludeDatabases)
	if err != nil {
		return nil, err
	}
	err = bs.targetMySQLService.StoreBranchMeta(schema, branchMeta) // this step is the commit point of BranchCreate function
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (bs *BranchService) BranchDiff() {
	// todo
	// SchemaDiff
	// query schemas from mysql
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
