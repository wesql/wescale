/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package sqlparser

// IsPureSelectStatement returns true if the query is a Select or Union statement without any Lock.
func IsPureSelectStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		if stmt.Lock == NoLock {
			return true
		}
	case *Union:
		if stmt.Lock == NoLock {
			return true
		}
	}

	return false
}

// ContainsLockStatement returns true if the query contains a Get Lock statement.
func ContainsLockStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		return isLockStatement(stmt)
	case *Union:
		return isLockStatement(stmt.Left) || isLockStatement(stmt.Right)
	}

	return false
}

// isLockStatement returns true if the query is a Get Lock statement.
func isLockStatement(stmt Statement) bool {
	s, ok := stmt.(*Select)
	if !ok {
		return false
	}
	foundLockingFunc := false
	err := Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *LockingFunc:
			foundLockingFunc = true
			return false, nil
		}
		return true, nil
	}, s)
	if err != nil {
		return false
	}
	return foundLockingFunc
}

func hasFuncInStatement(funcs []string, stmt Statement) bool {
	//return false if stmt is not a Select statement
	s, ok := stmt.(*Select)
	if !ok {
		return false
	}
	//visit the select statement and check if it is a Select Last Insert ID statement
	foundFunc := false
	err := Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *FuncExpr:
			for _, f := range funcs {
				if node.Name.Lowered() == f {
					foundFunc = true
					return false, nil
				}
			}
		}
		return true, nil
	}, s)
	if err != nil {
		return false
	}
	return foundFunc
}

// ContainsLastInsertIDStatement returns true if the query is a Select Last Insert ID statement.
func ContainsLastInsertIDStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		return isSelectLastInsertIDStatement(stmt)
	case *Union:
		return ContainsLastInsertIDStatement(stmt.Left) || ContainsLastInsertIDStatement(stmt.Right)
	}

	return false
}

// IsSelectLastInsertIDStatement returns true if the query is a Select Last Insert ID statement.
func isSelectLastInsertIDStatement(stmt Statement) bool {
	return hasFuncInStatement([]string{"last_insert_id"}, stmt)
}

// IsDDLStatement returns true if the query is an DDL statement.
func IsDDLStatement(stmt Statement) bool {
	return ASTToStatementType(stmt) == StmtDDL
}

// IsSelectForUpdateStatement returns true if the query is a Select For Update statement.
func IsSelectForUpdateStatement(stmt Statement) bool {
	switch stmt := stmt.(type) {
	case *Select:
		if stmt.Lock != NoLock {
			return true
		}
	case *Union:
		if stmt.Lock != NoLock {
			return true
		}
	}

	return false
}

// IsKillStatement returns true if the query is a Kill statement.
func IsKillStatement(_ Statement) bool {
	panic("implement me")
}
