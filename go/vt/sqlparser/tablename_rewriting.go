package sqlparser

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func RewriteTableName(in Statement, keyspace string) (Statement, bool, bool, error) {
	tr := newTableNameRewriter(keyspace)
	result := SafeRewrite(in, tr.rewriteDown, tr.rewriteUp)

	out, ok := result.(Statement)
	if !ok {
		return nil, false, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	return out, tr.skipUse, tr.rewriteSQL, nil
}

type tableRewriter struct {
	err       error
	inDerived bool
	cur       *Cursor

	skipUse    bool
	rewriteSQL bool
	keyspace   string
}

func newTableNameRewriter(keyspace string) *tableRewriter {
	return &tableRewriter{
		keyspace:   keyspace,
		skipUse:    true,
		rewriteSQL: true,
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
		return tr.visitDelete(node)
	case *OtherRead, *OtherAdmin:
		tr.skipUse = false
		tr.rewriteSQL = false
		return false
	case *Show, *With:
		tr.skipUse = false
		return false
	case *Use, *CallProc, *Begin, *Commit, *Rollback, *ColName,
		*Load, *Savepoint, *Release, *SRollback, *Set:
		return false
	case *AlterMigration, *RevertMigration, *ShowMigrationLogs,
		*ShowThrottledApps, *ShowThrottlerStatus:
		return false
	}
	return tr.err == nil
}

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
	}
	return tr.err == nil
}

func (tr *tableRewriter) rewriteUp(cursor *Cursor) bool {
	if tr.err != nil {
		return false
	}

	switch node := cursor.Node().(type) {
	case TableName:
		tr.rewriteTableName(node, cursor)
	}
	return true
}

func (tr *tableRewriter) rewriteTableName(node TableName, cursor *Cursor) {
	if node.Name.String() == "dual" {
		return
	}
	if node.Qualifier.IsEmpty() {
		node.Qualifier = NewIdentifierCS(tr.keyspace)
	}
	cursor.Replace(node)
}

func (tr *tableRewriter) visitDelete(node *Delete) bool {
	if node.With != nil && len(node.With.ctes) > 0 {
		tr.skipUse = false
		return false
	}
	for _, expr := range node.TableExprs {
		_ = SafeRewrite(expr, tr.rewriteDownSelect, tr.rewriteUp)
	}
	if node.Where != nil {
		_ = SafeRewrite(node.Where, tr.rewriteDownSelect, tr.rewriteUp)
	}
	if node.Partitions != nil {
		_ = SafeRewrite(node.Partitions, tr.rewriteDownSelect, tr.rewriteUp)
	}
	if node.OrderBy != nil {
		_ = SafeRewrite(node.OrderBy, tr.rewriteDownSelect, tr.rewriteUp)
	}
	if node.Limit != nil {
		if node.OrderBy != nil {
			_ = SafeRewrite(node.Limit, tr.rewriteDownSelect, tr.rewriteUp)
		}
	}
	return false
}
