/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/engine"
)

// collationInTable allows us to set a collation on a column
type collationInTable struct {
	ks, table, collationName string
	colName                  string
}

type collationTestCase struct {
	query      string
	check      func(t *testing.T, colls []collationInTable, primitive engine.Primitive)
	collations []collationInTable
}

func (tc *collationTestCase) run(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "vschemas/schema.json", false),
		sysVarEnabled: true,
		version:       Gen4,
	}

	tc.addCollationsToSchema(vschemaWrapper)
	plan, err := TestBuilder(tc.query, vschemaWrapper, vschemaWrapper.currentDb())
	require.NoError(t, err)
	tc.check(t, tc.collations, plan.Instructions)
}

func (tc *collationTestCase) addCollationsToSchema(vschema *vschemaWrapper) {
	for _, collation := range tc.collations {
		tbl := vschema.v.Keyspaces[collation.ks].Tables[collation.table]
		for i, c := range tbl.Columns {
			if c.Name.EqualString(collation.colName) {
				tbl.Columns[i].CollationName = collation.collationName
				break
			}
		}
	}
}
