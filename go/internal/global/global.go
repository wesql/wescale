/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package global

// Keyspace
const (
	DefaultKeyspace = "mysql"
	DefaultShard    = "0"
)

// Planner
const (
	Pushdown = "Pushdown"
)

const DefaultFlavor = "MySQL56"

// Schema Management
const (
	TableSchemaTracking = false
	ViewSchemaTracking  = false
)

const (
	// ReadWriteSplitEnablesREPLICA means Replica nodes can be used for reads in read-write-split.
	ReadWriteSplitEnablesREPLICA = true
	// ReadWriteSplitEnablesRDONLY means RdOnly nodes can be used for reads in read-write-split.
	ReadWriteSplitEnablesRDONLY = false
)

const (
	MysqlBased = "mysqlbased"
)

// AuthServer Management
const (
	AuthServerMysqlBased = MysqlBased
	AuthServerStatic     = "static"
)
