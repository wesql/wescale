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

func (er *tableRewriter) rewriteDown(node SQLNode, parent SQLNode) bool {
	// do not rewrite tableName if there is a WITH node
	if !er.skipUse {
		return false
	}
	switch node := node.(type) {
	case *Select:
		if node.With != nil && len(node.With.ctes) > 0 {
			er.skipUse = false
			return false
		}

		_, isDerived := parent.(*DerivedTable)
		var tmp bool
		tmp, er.inDerived = er.inDerived, isDerived
		_ = SafeRewrite(node, er.rewriteDownSelect, er.rewriteUp)
		er.inDerived = tmp
		return false
	case *OtherRead, *OtherAdmin, *Show, *With:
		er.skipUse = false
		return false
	case *Use, *CallProc, *Begin, *Commit, *Rollback,
		*Load, *Savepoint, *Release, *SRollback, *Set,
		Explain:
		return false
	case *AlterMigration, *RevertMigration, *ShowMigrationLogs,
		*ShowThrottledApps, *ShowThrottlerStatus:
		return false
	}
	return er.err == nil
}

func (er *tableRewriter) rewriteDownSelect(node SQLNode, parent SQLNode) bool {
	if er.skipUse {
		return false
	}

	switch node := node.(type) {
	case *Select:
		_, isDerived := parent.(*DerivedTable)
		if !isDerived {
			return true
		}
		var tmp bool
		tmp, er.inDerived = er.inDerived, isDerived
		_ = SafeRewrite(node, er.rewriteDownSelect, er.rewriteUp)
		// Don't continue
		er.inDerived = tmp
		return false
	case *ColName:
		return false
	case SelectExprs:
		return false
	}
	return er.err == nil
}

func (er *tableRewriter) rewriteUp(cursor *Cursor) bool {
	if er.err != nil {
		return false
	}

	switch node := cursor.Node().(type) {
	case TableName:
		er.rewriteTableName(node, cursor)
	}
	return true
}

func (er *tableRewriter) rewriteTableName(node TableName, cursor *Cursor) {
	if node.Name.String() == "dual" {
		return
	}
	if node.Qualifier.IsEmpty() {
		node.Qualifier = NewIdentifierCS(er.keyspace)
	}
	cursor.Replace(node)
}
