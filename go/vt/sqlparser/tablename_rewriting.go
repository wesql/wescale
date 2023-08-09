/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package sqlparser

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func RewriteTableName(in Statement, keyspace string) (Statement, bool, error) {
	tr := newTableNameRewriter(keyspace)
	result := SafeRewrite(in, tr.rewriteDown, tr.rewriteUp)

	out, ok := result.(Statement)
	if !ok {
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	return out, tr.skipUse, nil
}

type tableRewriter struct {
	err       error
	inDerived bool
	cur       *Cursor

	skipUse  bool
	keyspace string
}

func newTableNameRewriter(keyspace string) *tableRewriter {
	return &tableRewriter{
		keyspace: keyspace,
		skipUse:  true,
	}
}

func (tr *tableRewriter) rewriteDown(node SQLNode, parent SQLNode) bool {
	// do not rewrite tableName if there is a WITH node
	if !tr.skipUse {
		return false
	}
	switch node := node.(type) {
	case *Select:
		if node.With != nil && len(node.With.ctes) > 0 {
			tr.skipUse = false
			return false
		}

		_, isDerived := parent.(*DerivedTable)
		var tmp bool
		tmp, tr.inDerived = tr.inDerived, isDerived
		_ = SafeRewrite(node, tr.rewriteDownSelect, tr.rewriteUp)
		tr.inDerived = tmp
		return false
	case *Union:
		if node.With != nil && len(node.With.ctes) > 0 {
			tr.skipUse = false
			return false
		}
	case *Delete:
		if len(node.Targets) > 0 {
			tr.skipUse = false
			return false
		}
		if node.With != nil && len(node.With.ctes) > 0 {
			tr.skipUse = false
			return false
		}
		return true
	case *OtherRead, *OtherAdmin:
		// the table information is missing in the stmt.
		tr.skipUse = false
		return false
	case *ColName:
		return false
	case *Use, *CallProc, *Begin, *Commit, *Rollback,
		*Load, *Savepoint, *Release, *SRollback, *Show, *With, *CreateTable:
		tr.skipUse = false
		return false
	case *AlterMigration, *RevertMigration, *ShowMigrationLogs,
		*ShowThrottledApps, *ShowThrottlerStatus:
		tr.skipUse = false
		return false
	}
	return tr.err == nil
}

// rewriteDownSelect handle recursive select statement
func (tr *tableRewriter) rewriteDownSelect(node SQLNode, parent SQLNode) bool {
	if !tr.skipUse {
		return false
	}

	switch node := node.(type) {
	case *Select:
		_, isDerived := parent.(*DerivedTable)
		if !isDerived {
			return true
		}
		var tmp bool
		tmp, tr.inDerived = tr.inDerived, isDerived
		_ = SafeRewrite(node, tr.rewriteDownSelect, tr.rewriteUp)
		// Don't continue
		tr.inDerived = tmp
		return false
	case *ColName:
		return false
	case SelectExprs:
		for _, expr := range node {
			switch expr := expr.(type) {
			case *AliasedExpr:
				_ = SafeRewrite(expr.Expr, tr.rewriteDownSelect, tr.rewriteUp)
			}
		}
		return false
	}
	return tr.err == nil
}

func (tr *tableRewriter) rewriteUp(cursor *Cursor) bool {
	if tr.err != nil {
		return false
	}

	switch newnode := cursor.Node().(type) {
	case TableName:
		tr.rewriteTableName(newnode, cursor)
	}
	return true
}

func (tr *tableRewriter) rewriteTableName(newnode TableName, cursor *Cursor) {
	if newnode.Name.String() == "dual" {
		return
	}
	if newnode.Qualifier.IsEmpty() {
		newnode.Qualifier = NewIdentifierCS(tr.keyspace)
	}
	// till here, cursor holds the replacer handleFunc
	// replace original node with a new one
	cursor.Replace(newnode)
}
