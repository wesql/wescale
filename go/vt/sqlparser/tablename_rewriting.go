package sqlparser

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func RewriteTableName(in Statement, keyspace string) (Statement, error) {
	tr := newTableNameRewriter(keyspace)
	result := SafeRewrite(in, tr.rewriteDown, tr.rewriteUp)

	out, ok := result.(Statement)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	return out, nil
}

type tableRewriter struct {
	err       error
	inDerived bool
	cur       *Cursor

	keyspace string
}

func newTableNameRewriter(keyspace string) *tableRewriter {
	return &tableRewriter{
		keyspace: keyspace,
	}
}

func (er *tableRewriter) rewriteDown(node SQLNode, parent SQLNode) bool {
	switch node := node.(type) {
	case *Select:
		_, isDerived := parent.(*DerivedTable)
		var tmp bool
		tmp, er.inDerived = er.inDerived, isDerived
		_ = SafeRewrite(node, er.rewriteDownSelect, er.rewriteUp)
		er.inDerived = tmp
		return false
	case *OtherRead:
		//still need to use the "use" statement until the Table information is completed.
	case *Use, *OtherAdmin, *CallProc, *Begin, *Commit, *Rollback,
		*Load, *Savepoint, *Release, *SRollback, *Set, *Show,
		Explain:
		return false
	case *AlterMigration, *RevertMigration, *ShowMigrationLogs,
		*ShowThrottledApps, *ShowThrottlerStatus:
		return false
	}
	return er.err == nil
}

func (er *tableRewriter) rewriteDownSelect(node SQLNode, parent SQLNode) bool {
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
	case SelectExprs:
		return !er.inDerived
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
