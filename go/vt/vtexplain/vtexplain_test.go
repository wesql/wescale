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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

func defaultTestOpts() *Options {
	return &Options{
		ReplicationMode: "ROW",
		NumShards:       4,
		Normalize:       true,
		StrictDDL:       true,
	}
}

type testopts struct {
	shardmap map[string]map[string]*topo.ShardInfo
}

func initTest(mode string, opts *Options, topts *testopts, t *testing.T) *VTExplain {
	schema, err := os.ReadFile("testdata/test-schema.sql")
	require.NoError(t, err)

	vSchema, err := os.ReadFile("testdata/test-vschema.json")
	require.NoError(t, err)

	shardmap := ""
	if topts.shardmap != nil {
		shardmapBytes, err := json.Marshal(topts.shardmap)
		require.NoError(t, err)

		shardmap = string(shardmapBytes)
	}

	opts.ExecutionMode = mode
	vte, err := Init(string(vSchema), string(schema), shardmap, opts)
	require.NoError(t, err, "vtexplain CalculateDiff error\n%s", string(schema))
	return vte
}

func testExplain(testcase string, opts *Options, t *testing.T) {
	modes := []string{
		ModeMulti,

		// TwoPC mode is functional, but the output isn't stable for
		// tests since there are timestamps in the value rows
		// ModeTwoPC,
	}

	for _, mode := range modes {
		runTestCase(testcase, mode, opts, &testopts{}, t)
	}
}

func runTestCase(testcase, mode string, opts *Options, topts *testopts, t *testing.T) {
	t.Run(testcase, func(t *testing.T) {
		vte := initTest(mode, opts, topts, t)

		sqlFile := fmt.Sprintf("testdata/%s-queries.sql", testcase)
		sql, err := os.ReadFile(sqlFile)
		require.NoError(t, err, "vtexplain error")

		textOutFile := fmt.Sprintf("testdata/%s-output/%s-output.txt", mode, testcase)
		expected, _ := os.ReadFile(textOutFile)

		explains, err := vte.Run(string(sql))
		require.NoError(t, err, "vtexplain error")
		require.NotNil(t, explains, "vtexplain error running %s: no explain", string(sql))

		// We want to remove the additional `set collation_connection` queries that happen
		// when the tablet connects to MySQL to set the default collation.
		// Removing them lets us keep simpler expected output files.
		for _, e := range explains {
			for i, action := range e.TabletActions {
				var mysqlQueries []*MysqlQuery
				for _, query := range action.MysqlQueries {
					if !strings.Contains(strings.ToLower(query.SQL), "set collation_connection") {
						mysqlQueries = append(mysqlQueries, query)
					}
				}
				e.TabletActions[i].MysqlQueries = mysqlQueries
			}
		}

		explainText, err := vte.ExplainsAsText(explains)
		require.NoError(t, err, "vtexplain error")

		if diff := cmp.Diff(strings.TrimSpace(string(expected)), strings.TrimSpace(explainText)); diff != "" {
			// Print the Text that was actually returned and also dump to a
			// temp file to be able to diff the results.
			t.Errorf("Text output did not match (-want +got):\n%s", diff)

			testOutputTempDir, err := os.MkdirTemp("testdata", "plan_test")
			require.NoError(t, err)
			gotFile := fmt.Sprintf("%s/%s-output.txt", testOutputTempDir, testcase)
			os.WriteFile(gotFile, []byte(explainText), 0644)

			t.Logf("run the following command to update the expected output:")
			t.Logf("cp %s/* %s", testOutputTempDir, path.Dir(textOutFile))
		}
	})
}

func testShardInfo(ks, start, end string, primaryServing bool, t *testing.T) *topo.ShardInfo {
	kr, err := key.ParseKeyRangeParts(start, end)
	require.NoError(t, err)

	return topo.NewShardInfo(
		ks,
		fmt.Sprintf("%s-%s", start, end),
		&topodata.Shard{KeyRange: kr, IsPrimaryServing: primaryServing},
		&vtexplainTestTopoVersion{},
	)
}

func TestInit(t *testing.T) {
	vschema := `{
  "ks1": {
    "sharded": true,
    "tables": {
      "table_missing_primary_vindex": {}
    }
  }
}`
	schema := "create table table_missing_primary_vindex (id int primary key)"
	_, err := Init(vschema, schema, "", defaultTestOpts())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing primary col vindex")
}

type vtexplainTestTopoVersion struct{}

func (vtexplain *vtexplainTestTopoVersion) String() string { return "vtexplain-test-topo" }
