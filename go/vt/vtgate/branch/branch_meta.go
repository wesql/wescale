package branch

type BranchMeta struct {
	name string
	// source info
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	// filter rules
	includeDatabases []string
	excludeDatabases []string
	// others
	targetDBPattern string // todo
	status          string // todo
}

const (
	StatusInit = "init" // todo, need it?
)

type BranchSchema struct {
	// databases -> tables -> create table statement
	schema map[string]map[string]string
}

type DatabaseDiff struct {
	needCreate bool
	needDrop   bool
	// table name -> ddls to alter this table from origin to expected
	tableDDLs map[string][]string
}

type BranchDiff struct {
	// database name -> DatabaseDiff
	diffs map[string]*DatabaseDiff
}

const (
	InsertBranchSQL = `INSERT INTO mysql.branch 
		(name, source_host, source_port, source_user, source_password, includeDatabases, excludeDatabases, status, target_db_pattern) 
		VALUES ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')`

	InsertBranchSnapshotSQL = "insert into mysql.branch_snapshots(name, snapshot) values (%s, %s)"
)
