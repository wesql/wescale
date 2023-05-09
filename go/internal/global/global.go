/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package global

import "vitess.io/vitess/go/mysql"

// Keyspace
const (
	DefaultKeyspace = "_vt"
	DefaultShard    = "0"
)

// Planner
const (
	Pushdown = "Pushdown"
)

const DefaultFlavor = mysql.Mysql56FlavorID

// Schema Management
const (
	TableSchemaTracking = true
	ViewSchemaTracking  = true
)
