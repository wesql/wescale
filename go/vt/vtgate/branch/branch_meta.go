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
	targetDBPattern      string // todo
	status               BranchStatus
	IdOfNextDDLToExecute int // it's used in branch merge crash retry logic
}

type BranchStatus string

const (
	StatusUnknown BranchStatus = "unknown"
	StatusInit    BranchStatus = "init"
	StatusFetched BranchStatus = "fetched"
	statusCreated BranchStatus = "created"
)

func StringToBranchStatus(s string) BranchStatus {
	switch s {
	case "init":
		return StatusInit
	default:
		return StatusUnknown
	}
}

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
	UpsertBranchMetaSQL = `
    INSERT INTO mysql.branch 
        (name, source_host, source_port, source_user, source_password, 
        include_databases, exclude_databases, status, target_db_pattern, id_of_next_ddl_to_execute) 
    VALUES 
        ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s','%d')
    ON DUPLICATE KEY UPDATE 
        source_host = VALUES(source_host),
        source_port = VALUES(source_port),
        source_user = VALUES(source_user),
        source_password = VALUES(source_password),
        include_databases = VALUES(include_databases),
        exclude_databases = VALUES(exclude_databases),
        status = VALUES(status),
        target_db_pattern = VALUES(target_db_pattern),
        id_of_next_ddl_to_execute = VALUES(id_of_next_ddl_to_execute)`

	SelectBranchMetaSQL = "select * from mysql.branch where name='%s'"

	SelectBranchSnapshotSQL = "select * from mysql.branch_schema where name='%s' and schema_type='snapshot' order by id"

	DeleteBranchSnapshotSQL = "delete from mysql.branch_schema where name='%s' and schema_type='snapshot'"

	InsertBranchSnapshotSQL = "insert into mysql.branch_schema (name, database, table, sql, schema_type) values ('%s', '%s', '%s', '%s', 'snapshot')"
)
