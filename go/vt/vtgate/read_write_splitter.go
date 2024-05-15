/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"math/rand"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

func suggestTabletType(readWriteSplittingPolicy string, inTransaction, hasCreatedTempTables, hasAdvisoryLock bool, ratio int32, sql string, enableReadOnlyTransaction, isReadOnlyTx bool) (tabletType topodatapb.TabletType, err error) {
	if schema.NewReadWriteSplittingPolicy(readWriteSplittingPolicy).IsDisable() {
		return defaultTabletType, nil
	}

	if hasCreatedTempTables || hasAdvisoryLock {
		return defaultTabletType, nil
	}

	// in transaction but not read-only transaction
	if inTransaction && (!isReadOnlyTx || !enableReadOnlyTransaction) {
		return defaultTabletType, nil
	}

	shouldForceRouteToReadOnly := isReadOnlyTx && enableReadOnlyTransaction
	if shouldForceRouteToReadOnly {
		return topodatapb.TabletType_REPLICA, nil
	}

	support, err := isSQLSupportReadWriteSplit(sql)
	if err != nil {
		return defaultTabletType, err
	}
	if !support {
		return defaultTabletType, nil
	}
	// From now on, all statements can be routed to Replica/ReadOnly VTTablet
	return pickTabletTypeForReadWriteSplitting(ratio), nil
}

func pickTabletTypeForReadWriteSplitting(ratio int32) topodatapb.TabletType {
	return randomPickTabletType(ratio)
}

func randomPickTabletType(ratio int32) topodatapb.TabletType {
	if ratio == 0 {
		return defaultTabletType
	}
	percentage := float32(ratio) / 100
	if rand.Float32() > percentage {
		return defaultTabletType
	}
	return topodatapb.TabletType_REPLICA
}

// isSqlSupportReadWriteSplit : whether the query should be routed to a read-only vttablet
func isSQLSupportReadWriteSplit(query string) (bool, error) {
	s, _, err := sqlparser.Parse2(query)
	if err != nil {
		return false, err
	}

	// select last_insert_id() is a special case, it's not a read-only query
	if sqlparser.ContainsLastInsertIDStatement(s) {
		return false, nil
	}
	// GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS is a special case, it's not a read-only query
	if sqlparser.ContainsLockStatement(s) {
		return false, nil
	}
	// if hasSystemTable
	if hasSystemTable(s, "") {
		return false, nil
	}
	if sqlparser.IsPureSelectStatement(s) {
		return true, nil
	}
	return false, nil
}

func hasSystemTable(sel sqlparser.Statement, ksName string) bool {
	allTables := sqlparser.CollectTables(sel, ksName)
	for _, table := range allTables {
		if sqlparser.SystemSchema(table.GetSchema()) {
			return true
		}
	}
	return false
}
