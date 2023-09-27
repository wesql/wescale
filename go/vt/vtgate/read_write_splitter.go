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

func suggestTabletType(readWriteSplittingPolicy string, inTransaction, hasCreatedTempTables, hasAdvisoryLock bool, ratio int32, sql string, enableReadOnlyTransaction, isReadOnlyTx bool) (tabletType topodatapb.TabletType, err error) {
	suggestedTabletType := defaultTabletType
	if schema.NewReadWriteSplittingPolicy(readWriteSplittingPolicy).IsDisable() {
		return suggestedTabletType, nil
	}

	if hasCreatedTempTables || hasAdvisoryLock {
		return suggestedTabletType, nil
	}

	// isReadOnly determines whether the sql route to read-only node
	ro, err := isReadOnly(inTransaction, isReadOnlyTx, enableReadOnlyTransaction, sql, ratio)
	if err != nil {
		return suggestedTabletType, err
	}
	if ro { // if in read-only tx
		suggestedTabletType = topodatapb.TabletType_REPLICA
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
func isReadOnly(inTransaction, isReadOnlyTx, enableReadOnlyTransaction bool, query string, ratio int32) (bool, error) {
	if inTransaction {
		// if the sql is in transaction and its transaction access mode is read-only, then suggest type should be read-only tablet
		if isReadOnlyTx && enableReadOnlyTransaction {
			return true, nil
		}
		// in transaction but not read-only transaction
		return false, nil

	}

	s, _, err := sqlparser.Parse2(query)
	if err != nil {
		return false, err
	}
	// if the sql is like "start transaction read only"
	if beginStmt, ok := s.(*sqlparser.Begin); ok {
		if (len(beginStmt.TxAccessModes) == 1 && beginStmt.TxAccessModes[0] == sqlparser.ReadOnly) ||
			(len(beginStmt.TxAccessModes) == 2 && beginStmt.TxAccessModes[0] == sqlparser.ReadOnly && beginStmt.TxAccessModes[1] == sqlparser.WithConsistentSnapshot) ||
			(len(beginStmt.TxAccessModes) == 2 && beginStmt.TxAccessModes[1] == sqlparser.ReadOnly && beginStmt.TxAccessModes[0] == sqlparser.WithConsistentSnapshot) {
			return true, nil
		}
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
		if pickTabletTypeForReadWriteSplitting(ratio) == topodatapb.TabletType_PRIMARY {
			return false, nil
		}
		return true, nil

	}
	return false, nil
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
