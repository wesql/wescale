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

package vtexplain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrParseSchema(t *testing.T) {
	testSchema := `create table t1 like t2`
	ddl, err := parseSchema(testSchema, &Options{StrictDDL: true})
	require.NoError(t, err)

	_, err = newTabletEnvironment(ddl, defaultTestOpts())
	require.Error(t, err, "check your schema, table[t2] doesn't exist")
}
