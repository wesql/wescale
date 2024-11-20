package branch

type BranchMeta struct {
	name string
	// source info
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	// filter rules
	include string
	exclude string
	// others
	targetDBPattern string // todo
	status          string // todo
}

const (
	InsertBranchSQL = `INSERT INTO mysql.branch 
		(name, source_host, source_port, source_user, source_password, include, exclude, status, target_db_pattern) 
		VALUES ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')`

	InsertBranchSnapshotSQL = "insert into mysql.branch_snapshots(name, snapshot) values (%s, %s)"
)
