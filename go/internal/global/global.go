/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package global

import "time"

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
	TableSchemaTracking = true
	ViewSchemaTracking  = false

	SignalSchemaChangeReloadIntervalSeconds = 5
	// should configure this in vtgate.cnf
	HealthCheckTimeoutSeconds = 60
)

const (
	MysqlBased = "mysqlbased"
)

// AuthServer Management
const (
	AuthServerMysqlBased = MysqlBased
	AuthServerStatic     = "static"
	AuthServerNone       = "none"
)

// ACL
const (
	TableACLModeMysqlBased   = MysqlBased
	TableACLModeSimple       = "simple"
	DefaultACLReloadInterval = 5 * time.Second
)

const (
	PutFailPoint    = "put_failpoint"
	RemoveFailPoint = "remove_failpoint"
)

const (
	TopoServerConfigOverwriteShard = true
)
