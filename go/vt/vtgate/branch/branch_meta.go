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
	CreateBranchMetaTableSQL = `CREATE TABLE IF NOT EXISTS mysql.branch (
			id bigint auto_increment,
			name varchar(64) not null,
			source_host varchar(32) not null,
			source_port int not null,
			source_user varchar(64),
			source_password varchar(64),
			include varchar(256) not null,
			exclude varchar(256),
			status varchar(32) not null,
			target_db_pattern varchar(256),
			primary key (id),
			unique key (name)
		)ENGINE=InnoDB;`

	InsertBranchSQL = `INSERT INTO mysql.branch 
		(name, source_host, source_port, source_user, source_password, include, exclude, status, target_db_pattern) 
		VALUES ('%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')`

	CreateBranchSnapshotTableSQL = `CREATE TABLE IF NOT EXISTS mysql.branch_snapshots(
			id bigint unsigned NOT NULL AUTO_INCREMENT,
			name varchar(64) NOT NULL,
			snapshot                longblob,
			PRIMARY KEY (id),
			UNIQUE KEY(name)
		) ENGINE = InnoDB;`

	InsertBranchSnapshotSQL = "insert into mysql.branch_snapshots(name, snapshot) values (%s, %s)"
)
