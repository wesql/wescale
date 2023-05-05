/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func rewriteSingleTbl(del *sqlparser.Delete) (*sqlparser.Delete, error) {
	atExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return del, nil
	}
	if !atExpr.As.IsEmpty() && !sqlparser.Equals.IdentifierCS(del.Targets[0].Name, atExpr.As) {
		// Unknown table in MULTI DELETE
		return nil, vterrors.VT03003(del.Targets[0].Name.String())
	}

	tbl, ok := atExpr.Expr.(sqlparser.TableName)
	if !ok {
		// derived table
		return nil, vterrors.VT03004(atExpr.As.String())
	}
	if atExpr.As.IsEmpty() && !sqlparser.Equals.IdentifierCS(del.Targets[0].Name, tbl.Name) {
		// Unknown table in MULTI DELETE
		return nil, vterrors.VT03003(del.Targets[0].Name.String())
	}

	del.TableExprs = sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: tbl}}
	del.Targets = nil
	if del.Where != nil {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			col, ok := node.(*sqlparser.ColName)
			if !ok {
				return true, nil
			}
			if !col.Qualifier.IsEmpty() {
				col.Qualifier = tbl
			}
			return true, nil
		}, del.Where)
	}
	return del, nil
}
