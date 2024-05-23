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

package vexec

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wesql/wescale/go/vt/sqlparser"
	"github.com/wesql/wescale/go/vt/vtctl/workflow/vexec/testutil"
)

func TestVReplicationQueryPlanner_PlanQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		err   error
	}{
		{
			name:  "basic select",
			query: "SELECT id FROM mysql.vreplication",
			err:   nil,
		},
		{
			name:  "insert not supported",
			query: "INSERT INTO mysql.vreplication (id) VALUES (1)",
			err:   ErrUnsupportedQuery,
		},
		{
			name:  "basic update",
			query: "UPDATE mysql.vreplication SET workflow = 'my workflow'",
			err:   nil,
		},
		{
			name:  "basic delete",
			query: "DELETE FROM mysql.vreplication",
			err:   nil,
		},
		{
			name:  "other query",
			query: "CREATE TABLE foo (id INT(11) PRIMARY KEY NOT NULL) ENGINE=InnoDB",
			err:   ErrUnsupportedQuery,
		},
	}

	planner := NewVReplicationQueryPlanner(nil, "", "")

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmt := testutil.StatementFromString(t, tt.query)

			_, err := planner.PlanQuery(stmt)
			if tt.err != nil {
				assert.True(t, errors.Is(err, tt.err), "expected err of type %v, got %v", tt.err, err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestVReplicationQueryPlanner_planSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		query                string
		expectedPlannedQuery string
	}{
		{
			name:                 "simple select",
			query:                "SELECT id FROM mysql.vreplication WHERE id > 10",
			expectedPlannedQuery: "SELECT id FROM mysql.vreplication WHERE id > 10 AND db_name = 'testkeyspace' AND workflow = 'testworkflow'",
		},
		{
			name:                 "select with workflow and dbname columns already in WHERE",
			query:                "SELECT id FROM mysql.vreplication WHERE id > 10 AND db_name = 'testkeyspace' AND workflow = 'testworkflow'",
			expectedPlannedQuery: "SELECT id FROM mysql.vreplication WHERE id > 10 AND db_name = 'testkeyspace' AND workflow = 'testworkflow'",
		},
		{
			// In this case, the QueryParams for the planner (which have
			// workflow = "testworkflow"; db_name = "testkeyspace") are
			// ignored because the WHERE clause was explicit.
			name:                 "select with workflow and dbname columns with different values",
			query:                "SELECT id FROM mysql.vreplication WHERE id > 10 AND db_name = 'different_keyspace' AND workflow = 'otherworkflow'",
			expectedPlannedQuery: "SELECT id FROM mysql.vreplication WHERE id > 10 AND db_name = 'different_keyspace' AND workflow = 'otherworkflow'",
		},
	}

	planner := NewVReplicationQueryPlanner(nil, "testworkflow", "testkeyspace")

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmt := testutil.StatementFromString(t, tt.query)
			qp, err := planner.PlanQuery(stmt)

			assert.NoError(t, err)
			fixedqp, ok := qp.(*FixedQueryPlan)
			require.True(t, ok, "VReplicationQueryPlanner should always return a FixedQueryPlan from PlanQuery, got %T", qp)
			assert.Equal(t, testutil.ParsedQueryFromString(t, tt.expectedPlannedQuery), fixedqp.ParsedQuery)
		})
	}
}

func TestVReplicationQueryPlanner_planUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		planner              *VReplicationQueryPlanner
		query                string
		expectedPlannedQuery string
		expectedErr          error
	}{
		{
			name:                 "simple update",
			planner:              NewVReplicationQueryPlanner(nil, "testworkflow", "testkeyspace"),
			query:                "UPDATE mysql.vreplication SET state = 'Running'",
			expectedPlannedQuery: "UPDATE mysql.vreplication SET state = 'Running' WHERE db_name = 'testkeyspace' AND workflow = 'testworkflow'",
			expectedErr:          nil,
		},
		{
			name:        "including an ORDER BY is an error",
			planner:     NewVReplicationQueryPlanner(nil, "", ""),
			query:       "UPDATE mysql.vreplication SET state = 'Running' ORDER BY id DESC",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
		{
			name:        "including a LIMIT is an error",
			planner:     NewVReplicationQueryPlanner(nil, "", ""),
			query:       "UPDATE mysql.vreplication SET state = 'Running' LIMIT 5",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
		{
			name:        "cannot update id column",
			planner:     NewVReplicationQueryPlanner(nil, "", "testkeyspace"),
			query:       "UPDATE mysql.vreplication SET id = 5",
			expectedErr: ErrCannotUpdateImmutableColumn,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmt := testutil.StatementFromString(t, tt.query)

			qp, err := tt.planner.PlanQuery(stmt)
			if tt.expectedErr != nil {
				assert.True(t, errors.Is(err, tt.expectedErr), "expected err of type %q, got %q", tt.expectedErr, err)

				return
			}

			fixedqp, ok := qp.(*FixedQueryPlan)
			require.True(t, ok, "VReplicationQueryPlanner should always return a FixedQueryPlan from PlanQuery, got %T", qp)
			assert.Equal(t, testutil.ParsedQueryFromString(t, tt.expectedPlannedQuery), fixedqp.ParsedQuery)
		})
	}
}

func TestVReplicationQueryPlanner_planDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		query                string
		expectedPlannedQuery string
		expectedErr          error
	}{
		{
			name:                 "simple delete",
			query:                "DELETE FROM mysql.vreplication WHERE id = 1",
			expectedPlannedQuery: "DELETE FROM mysql.vreplication WHERE id = 1 AND db_name = 'testkeyspace'",
			expectedErr:          nil,
		},
		{
			name:        "DELETE with USING clause is not supported",
			query:       "DELETE FROM mysql.vreplication, mysql.schema_migrations USING mysql.vreplication INNER JOIN mysql.schema_migrations",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
		{
			name:        "DELETE with a PARTITION clause is not supported",
			query:       "DELETE FROM mysql.vreplication PARTITION (p1)",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
		{
			name:        "DELETE with ORDER BY is not supported",
			query:       "DELETE FROM mysql.vreplication ORDER BY id DESC",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
		{
			name:        "DELETE with LIMIT is not supported",
			query:       "DELETE FROM mysql.vreplication LIMIT 5",
			expectedErr: ErrUnsupportedQueryConstruct,
		},
	}

	planner := NewVReplicationQueryPlanner(nil, "", "testkeyspace")

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmt := testutil.StatementFromString(t, tt.query)

			qp, err := planner.PlanQuery(stmt)
			if tt.expectedErr != nil {
				assert.True(t, errors.Is(err, tt.expectedErr), "expected err of type %q, got %q", tt.expectedErr, err)

				return
			}

			fixedqp, ok := qp.(*FixedQueryPlan)
			require.True(t, ok, "VReplicationQueryPlanner should always return a FixedQueryPlan from PlanQuery, got %T", qp)
			assert.Equal(t, testutil.ParsedQueryFromString(t, tt.expectedPlannedQuery), fixedqp.ParsedQuery)
		})
	}
}

func TestVReplicationLogQueryPlanner(t *testing.T) {
	t.Parallel()

	t.Run("planSelect", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name            string
			targetStreamIDs map[string][]int64
			query           string
			assertion       func(t *testing.T, plan QueryPlan)
			shouldErr       bool
		}{
			{
				targetStreamIDs: map[string][]int64{
					"a": {1, 2},
				},
				query: "select * from mysql.vreplication_log",
				assertion: func(t *testing.T, plan QueryPlan) {
					t.Helper()
					qp, ok := plan.(*PerTargetQueryPlan)
					if !ok {
						require.FailNow(t, "failed type check", "expected plan to be PerTargetQueryPlan, got %T: %v", plan, plan)
					}

					expected := map[string]string{
						"a": "select * from mysql.vreplication_log where vrepl_id in (1, 2)",
					}
					assertQueryMapsMatch(t, expected, qp.ParsedQueries)
				},
			},
			{
				targetStreamIDs: map[string][]int64{
					"a": nil,
				},
				query: "select * from mysql.vreplication_log",
				assertion: func(t *testing.T, plan QueryPlan) {
					t.Helper()
					qp, ok := plan.(*PerTargetQueryPlan)
					if !ok {
						require.FailNow(t, "failed type check", "expected plan to be PerTargetQueryPlan, got %T: %v", plan, plan)
					}

					expected := map[string]string{
						"a": "select * from mysql.vreplication_log where 1 != 1",
					}
					assertQueryMapsMatch(t, expected, qp.ParsedQueries)
				},
			},
			{
				targetStreamIDs: map[string][]int64{
					"a": {1},
				},
				query: "select * from mysql.vreplication_log",
				assertion: func(t *testing.T, plan QueryPlan) {
					t.Helper()
					qp, ok := plan.(*PerTargetQueryPlan)
					if !ok {
						require.FailNow(t, "failed type check", "expected plan to be PerTargetQueryPlan, got %T: %v", plan, plan)
					}

					expected := map[string]string{
						"a": "select * from mysql.vreplication_log where vrepl_id = 1",
					}
					assertQueryMapsMatch(t, expected, qp.ParsedQueries)
				},
			},
			{
				query: "select * from mysql.vreplication_log where vrepl_id = 1",
				assertion: func(t *testing.T, plan QueryPlan) {
					t.Helper()
					qp, ok := plan.(*FixedQueryPlan)
					if !ok {
						require.FailNow(t, "failed type check", "expected plan to be FixedQueryPlan, got %T: %v", plan, plan)
					}

					assert.Equal(t, "select * from mysql.vreplication_log where vrepl_id = 1", qp.ParsedQuery.Query)
				},
			},
			{
				targetStreamIDs: map[string][]int64{
					"a": {1, 2},
				},
				query: "select * from mysql.vreplication_log where foo = 'bar'",
				assertion: func(t *testing.T, plan QueryPlan) {
					t.Helper()
					qp, ok := plan.(*PerTargetQueryPlan)
					if !ok {
						require.FailNow(t, "failed type check", "expected plan to be PerTargetQueryPlan, got %T: %v", plan, plan)
					}

					expected := map[string]string{
						"a": "select * from mysql.vreplication_log where vrepl_id in (1, 2) and foo = 'bar'",
					}
					assertQueryMapsMatch(t, expected, qp.ParsedQueries)
				},
			},
		}

		for _, tt := range tests {
			tt := tt

			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				planner := NewVReplicationLogQueryPlanner(nil, tt.targetStreamIDs)
				stmt, err := sqlparser.Parse(tt.query)
				require.NoError(t, err, "could not parse query %q", tt.query)
				qp, err := planner.planSelect(stmt.(*sqlparser.Select))
				if tt.shouldErr {
					assert.Error(t, err)
					return
				}

				tt.assertion(t, qp)
			})
		}
	})
}

func assertQueryMapsMatch(t *testing.T, expected map[string]string, actual map[string]*sqlparser.ParsedQuery, msgAndArgs ...any) {
	t.Helper()

	actualQueryMap := make(map[string]string, len(actual))
	for k, v := range actual {
		actualQueryMap[k] = v.Query
	}

	assert.Equal(t, expected, actualQueryMap, msgAndArgs...)
}
