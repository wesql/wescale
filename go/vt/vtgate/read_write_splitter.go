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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func suggestTabletType(readWriteSplittingPolicy string, inTransaction, hasCreatedTempTables, hasAdvisoryLock bool, ratio int32, sql string) (tabletType topodatapb.TabletType, err error) {
	suggestedTabletType := defaultTabletType
	if schema.NewReadWriteSplittingPolicy(readWriteSplittingPolicy).IsDisable() {
		return suggestedTabletType, nil
	}
	if inTransaction || hasCreatedTempTables || hasAdvisoryLock {
		return suggestedTabletType, nil
	}
	// if not in transaction, and the query is read-only, use REPLICA
	ro, err := isReadOnly(sql)
	if err != nil {
		return suggestedTabletType, err
	}
	if ro {
		suggestedTabletType = pickTabletTypeForReadWriteSplitting(ratio)
	}
	return suggestedTabletType, nil
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

// IsReadOnly : whether the query should be routed to a read-only vttablet
func isReadOnly(query string) (bool, error) {
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
	return sqlparser.IsPureSelectStatement(s), nil
}

func hasSystemTable(sel sqlparser.Statement, ksName string) bool {
	semTable, err := semantics.Analyze(sel, ksName, &semantics.FakeSI{})
	if err != nil {
		return false
	}
	for _, tableInfo := range semTable.Tables {
		if tableInfo.IsInfSchema() {
			return true
		}
	}
	return false
}
