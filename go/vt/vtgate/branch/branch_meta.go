package branch

import "vitess.io/vitess/go/vt/schemadiff"

type BranchMeta struct {
	Name string
	// source info
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	// filter rules
	IncludeDatabases []string
	ExcludeDatabases []string
	// others
	TargetDBPattern string // todo enhancement: so that we can test branch in single mysql instance
	Status          BranchStatus
}

type BranchStatus string

const (
	StatusUnknown BranchStatus = "unknown"
	StatusInit    BranchStatus = "init"
	StatusFetched BranchStatus = "fetched"
	StatusCreated BranchStatus = "created"

	StatusPreparing BranchStatus = "preparing"
	StatusPrepared  BranchStatus = "prepared"
	StatusMerging   BranchStatus = "merging"
	StatusMerged    BranchStatus = "merged"
)

func StringToBranchStatus(s string) BranchStatus {
	switch s {
	case "init":
		return StatusInit
	default:
		return StatusUnknown
	}
}

type MergeBackOption string

const (
	MergeOverride MergeBackOption = "override"
	MergeDiff     MergeBackOption = "diff"
)

type BranchDiffObjectsFlag string

const (
	FromSourceToTarget   BranchDiffObjectsFlag = "source_target" // diff from source schema to target schema
	FromTargetToSource   BranchDiffObjectsFlag = "target_source"
	FromSourceToSnapshot BranchDiffObjectsFlag = "source_snapshot"
	FromSnapshotToSource BranchDiffObjectsFlag = "snapshot_source"
	FromTargetToSnapshot BranchDiffObjectsFlag = "target_snapshot"
	FromSnapshotToTarget BranchDiffObjectsFlag = "snapshot_target"
)

type BranchShowOption string

const (
	ShowAll      BranchShowOption = "all"
	ShowSnapshot BranchShowOption = "snapshot"
	ShowStatus   BranchShowOption = "Status"
)

type BranchSchema struct {
	// databases -> tables -> create table statement or DDLs
	branchSchema map[string]map[string]string
}

type DatabaseDiff struct {
	NeedCreateDatabase bool
	NeedDropDatabase   bool
	// table Name -> ddls to create, drop or alter this table from origin to expected
	TableDDLs map[string][]string

	// table Name -> EntityDiffs, used in schema merge back conflict check
	tableEntityDiffs map[string]schemadiff.EntityDiff
}

type BranchDiff struct {
	// database Name -> DatabaseDiff
	Diffs map[string]*DatabaseDiff
}

const (
	// branch meta related

	UpsertBranchMetaSQL = `
    INSERT INTO mysql.branch 
        (Name, source_host, source_port, source_user, source_password, 
        include_databases, exclude_databases, Status, target_db_pattern) 
    VALUES 
        ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')
    ON DUPLICATE KEY UPDATE 
        source_host = VALUES(source_host),
        source_port = VALUES(source_port),
        source_user = VALUES(source_user),
        source_password = VALUES(source_password),
        include_databases = VALUES(include_databases),
        exclude_databases = VALUES(exclude_databases),
        Status = VALUES(Status),
        target_db_pattern = VALUES(target_db_pattern)`

	SelectBranchMetaSQL = "select * from mysql.branch where Name='%s'"

	InsertBranchMetaSQL = `INSERT INTO mysql.branch 
        (Name, source_host, source_port, source_user, source_password, 
        include_databases, exclude_databases, Status, target_db_pattern) 
    VALUES 
        ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')`

	UpdateBranchStatusSQL = "update mysql.branch set Status='%s' where Name='%s'"

	// snapshot related

	SelectBranchSnapshotSQL = "select * from mysql.branch_snapshot where Name='%s' order by id"

	DeleteBranchSnapshotSQL = "delete from mysql.branch_snapshot where Name='%s'"

	InsertBranchSnapshotSQL = "insert into mysql.branch_snapshot (Name, database, table, create_table_sql) values ('%s', '%s', '%s', '%s')"

	// merge back ddl related

	DeleteBranchMergeBackDDLSQL = "delete from mysql.branch_patch where Name='%s'"

	SelectBranchUnmergedDDLSQL = "select * from mysql.branch_patch where Name='%s' and merged = false order by id"

	InsertBranchMergeBackDDLSQL = "insert into mysql.branch_patch (Name, database, table, ddl, merged) values ('%s', '%s', '%s', '%s', false)"

	UpdateBranchMergeBackDDLMergedSQL = "update mysql.branch_patch set merged = true where id = '%d'"
)
