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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestUpdateFromSubQuery(t *testing.T) {
	executor, _, _, scblookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Update by primary vindex, but first execute subquery
	_, err := executorExec(executor, "update user set a=(select count(*) from user where id = 3) where id = 1", nil)
	require.NoError(t, err)
	wanted := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = (select count(*) from `user` where id = 3) where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, scblookup, wanted)
	testQueryLog(t, logChan, "TestExecute", "UPDATE", "update user set a=(select count(*) from user where id = 3) where id = 1", 1)
}

func TestUpdateInTransactionLookupDefaultReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, _, _, sbcLookup := createCustomExecutorSetValues(executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(
		executor,
		"update t2_lookup set lu_col = 5 where nv_lu_col = 2",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update t2_lookup set lu_col = 5 where nv_lu_col = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	assertQueries(t, sbcLookup, wantQueries)
}

func TestUpdateInTransactionLookupExclusiveReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, _, _, sbcLookup := createCustomExecutorSetValues(executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(
		executor,
		"select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 for update\n",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)

	_, err = executorExecSession(
		executor,
		"update t2_lookup set lu_col = 5 where erl_lu_col = 2",
		nil,
		safeSession.Session,
	)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 for update",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "update t2_lookup set lu_col = 5 where erl_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	require.NoError(t, err)
	assertQueries(t, sbcLookup, wantQueries)
}

func TestUpdateInTransactionLookupSharedReadLock(t *testing.T) {
	res := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|wo_lu_col|erl_lu_col|srl_lu_col|nrl_lu_col|nv_lu_col|lu_col|t2_lu_vdx",
			"int64|int64|int64|int64|int64|int64|int64|int64",
		),
		"1|2|2|2|2|2|1|0",
	)}
	executor, _, _, sbcLookup := createCustomExecutorSetValues(executorVSchema, res)

	safeSession := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err := executorExecSession(
		executor,
		"select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 lock in share mode\n",
		nil,
		safeSession.Session,
	)

	require.NoError(t, err)

	_, err = executorExecSession(
		executor,
		"update t2_lookup set lu_col = 5 where erl_lu_col = 2",
		nil,
		safeSession.Session,
	)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select id, wo_lu_col, erl_lu_col, srl_lu_col, nrl_lu_col, nv_lu_col, lu_col, lu_col = 5 from t2_lookup where nv_lu_col = 2 and lu_col = 1 lock in share mode",
			BindVariables: map[string]*querypb.BindVariable{},
		},
		{
			Sql:           "update t2_lookup set lu_col = 5 where erl_lu_col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}

	require.NoError(t, err)
	assertQueries(t, sbcLookup, wantQueries)
}

func TestUpdateComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update `user` set a = 2 where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc1, wantQueries)
	assertQueries(t, sbc2, nil)
}

func TestUpdateNormalize(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// Rewrite queries with bind vars.
	executor.normalize = true
	_, err := executorExec(executor, "/* leading */ update user set a=2 where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ update `user` set a = :a where id = :id /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"a":  sqltypes.TestBindVariable(int64(2)),
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestDeleteComments(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "Id", Type: sqltypes.Int64},
			{Name: "name", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarChar("myname"),
		}},
	}})
	_, err := executorExec(executor, "delete from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from `user` where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertOnDupKey(t *testing.T) {
	// This test just sanity checks that the statement is getting passed through
	// correctly. The full set of use cases are covered by TestInsertShardedIgnore.
	executor, _, _, sbclookup := createExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "int64|varbinary"),
		"1|1",
	)})
	query := "insert into insert_ignore_test(pv, owned, verify) values (1, 1, 1) on duplicate key update col = 2"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into insert_ignore_test(pv, owned, verify) values (1, 1, 1) on duplicate key update col = 2",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestAutocommitFail(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	query := "insert into user (id) values (1)"
	sbclookup.MustFailCodes[vtrpcpb.Code_ALREADY_EXISTS] = 1
	primarySession.Reset()
	primarySession.Autocommit = true
	defer func() {
		primarySession.Autocommit = false
	}()
	_, err := executorExec(executor, query, nil)
	require.Error(t, err)

	// make sure we have closed and rolled back any transactions started
	assert.False(t, primarySession.InTransaction, "left with tx open")
}

func TestInsertComments(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "insert into user(id, v, name) values (1, 2, 'myname') /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into `user`(id, v, `name`) values (1, 2, 'myname') /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestInsertGeneratorUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	// Fake a mysql auto-inc response.
	wantResult := &sqltypes.Result{
		InsertID:     1,
		RowsAffected: 1,
	}
	sbclookup.SetResults([]*sqltypes.Result{wantResult})

	result, err := executorExec(executor, "insert into main1(id, name) values (null, 'myname')", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into main1(id, `name`) values (null, 'myname')",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	utils.MustMatch(t, wantResult, result)
}

func TestInsertAutoincUnsharded(t *testing.T) {
	router, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Fake a mysql auto-inc response.
	query := "insert into `simple`(val) values ('val')"
	wantResult := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     2,
	}
	sbclookup.SetResults([]*sqltypes.Result{wantResult})

	result, err := executorExec(router, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
	assert.Equal(t, result, wantResult)

	testQueryLog(t, logChan, "TestExecute", "INSERT", "insert into `simple`(val) values ('val')", 1)
}

// If a statement gets broken up into two, and the first one fails,
// then an error should be returned normally.
func TestInsertPartialFail1(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// Make the first DML fail, there should be no rollback.
	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1

	_, err := executor.Execute(
		context.Background(),
		"TestExecute",
		NewSafeSession(&vtgatepb.Session{InTransaction: true}),
		"insert into user(id, v, name) values (1, 2, 'myname')",
		nil,
	)
	require.Error(t, err)
}

func assertQueriesContain(t *testing.T, sql, sbcName string, sbc *sandboxconn.SandboxConn) {
	t.Helper()
	expectedQuery := []*querypb.BoundQuery{{
		Sql:           sql,
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbc, expectedQuery)
}

// Prepared statement tests
func TestUpdateEqualWithPrepare(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorPrepare(executor, "update music set a = :a0 where id = :id0", map[string]*querypb.BindVariable{
		"a0":  sqltypes.Int64BindVariable(3),
		"id0": sqltypes.Int64BindVariable(2),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	assertQueries(t, sbclookup, wantQueries)
}

func TestDeleteEqualWithPrepare(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	_, err := executorPrepare(executor, "delete from user where id = :id0", map[string]*querypb.BindVariable{
		"id0": sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	var wantQueries []*querypb.BoundQuery

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateLastInsertID(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true

	sql := "update user set a = last_insert_id() where id = 1"
	primarySession.LastInsertId = 43
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "update `user` set a = :__lastInsertId where id = :id",
		BindVariables: map[string]*querypb.BindVariable{
			"__lastInsertId": sqltypes.Uint64BindVariable(43),
			"id":             sqltypes.Int64BindVariable(1)},
	}}

	assertQueries(t, sbclookup, wantQueries)
}

func TestUpdateReference(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "update zip_detail set status = 'CLOSED' where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "update zip_detail set `status` = 'CLOSED' where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, logChan, "TestExecute", "UPDATE", "update zip_detail set status = 'CLOSED' where id = 1", 1)
}

func TestDeleteReference(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "delete from zip_detail where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "delete from zip_detail where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	testQueryLog(t, logChan, "TestExecute", "DELETE", "delete from zip_detail where id = 1", 1)
}

func TestReservedConnDML(t *testing.T) {
	executor, _, _, sbc := createExecutorEnv()

	logChan := QueryLogger.Subscribe("TestReservedConnDML")
	defer QueryLogger.Unsubscribe(logChan)

	ctx := context.Background()
	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true})

	_, err := executor.Execute(ctx, "TestReservedConnDML", session, "use "+KsTestDefaultShard, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{
		{Sql: "set @@default_week_format = 1", BindVariables: map[string]*querypb.BindVariable{}},
	}
	sbc.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	sbc.Queries = nil
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "set default_week_format = 1", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	wantQueries = []*querypb.BoundQuery{
		{Sql: "insert into `simple`() values ()", BindVariables: map[string]*querypb.BindVariable{}},
	}
	sbc.Queries = nil
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "insert into `simple`() values ()", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "begin", nil)
	require.NoError(t, err)

	sbc.EphemeralShardErr = mysql.NewSQLError(mysql.CRServerGone, mysql.SSNetError, "connection gone")
	// as the first time the query fails due to connection loss i.e. reserved conn lost. It will be recreated to set statement will be executed again.
	wantQueries = []*querypb.BoundQuery{
		{Sql: "insert into `simple`() values ()", BindVariables: map[string]*querypb.BindVariable{}},
	}
	sbc.Queries = nil
	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "insert into `simple`() values ()", nil)
	require.NoError(t, err)
	assertQueries(t, sbc, wantQueries)

	_, err = executor.Execute(ctx, "TestReservedConnDML", session, "commit", nil)
	require.NoError(t, err)
}

func TestStreamingDML(t *testing.T) {
	method := "TestStreamingDML"

	executor, _, _, sbc := createExecutorEnv()

	logChan := QueryLogger.Subscribe(method)
	defer QueryLogger.Unsubscribe(logChan)

	ctx := context.Background()
	session := NewAutocommitSession(&vtgatepb.Session{})

	tcases := []struct {
		query  string
		result *sqltypes.Result

		inTx        bool
		openTx      bool
		changedRows int
		commitCount int
		expQuery    []*querypb.BoundQuery
	}{{
		query: "begin",

		inTx:     true,
		expQuery: []*querypb.BoundQuery{},
	}, {
		query:  "insert into `simple`() values ()",
		result: &sqltypes.Result{RowsAffected: 1},

		inTx:        true,
		openTx:      true,
		changedRows: 1,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "insert into `simple`() values ()",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query:  "update `simple` set name = 'V' where col = 2",
		result: &sqltypes.Result{RowsAffected: 3},

		inTx:        true,
		openTx:      true,
		changedRows: 3,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "update `simple` set `name` = 'V' where col = 2",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query:  "delete from `simple`",
		result: &sqltypes.Result{RowsAffected: 12},

		inTx:        true,
		openTx:      true,
		changedRows: 12,
		expQuery: []*querypb.BoundQuery{{
			Sql:           "delete from `simple`",
			BindVariables: map[string]*querypb.BindVariable{},
		}},
	}, {
		query: "commit",

		commitCount: 1,
		expQuery:    []*querypb.BoundQuery{},
	}}

	var qr *sqltypes.Result
	for _, tcase := range tcases {
		sbc.Queries = nil
		sbc.SetResults([]*sqltypes.Result{tcase.result})
		err := executor.StreamExecute(ctx, method, session, tcase.query, nil, func(result *sqltypes.Result) error {
			qr = result
			return nil
		})
		require.NoError(t, err)
		// should tx start
		assert.Equal(t, tcase.inTx, session.GetInTransaction())
		// row affected as returned by result
		assert.EqualValues(t, tcase.changedRows, qr.RowsAffected)
		// match the query received on tablet
		assertQueries(t, sbc, tcase.expQuery)
	}
}

func TestInsertSelectFromDual(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("TestInsertSelect")
	defer QueryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})

	query := "insert into user(id, v, name) select 1, 2, 'myname' from dual"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into `user`(id, v, `name`) select 1, 2, 'myname' from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	for _, workload := range []string{"olap", "oltp"} {
		sbclookup.Queries = nil
		wQuery := fmt.Sprintf("set @@workload = %s", workload)
		_, err := executor.Execute(context.Background(), "TestInsertSelect", session, wQuery, nil)
		require.NoError(t, err)

		sbclookup.Queries = nil
		_, err = executor.Execute(context.Background(), "TestInsertSelect", session, query, nil)
		require.NoError(t, err)

		assertQueries(t, sbclookup, wantQueries)

		testQueryLog(t, logChan, "TestInsertSelect", "SET", wQuery, 0)
		testQueryLog(t, logChan, "TestInsertSelect", "INSERT", "insert into user(id, v, name) select 1, 2, 'myname' from dual", 1)
	}
}

func TestInsertSelectFromTable(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	logChan := QueryLogger.Subscribe("TestInsertSelect")
	defer QueryLogger.Unsubscribe(logChan)

	session := NewAutocommitSession(&vtgatepb.Session{})

	query := "insert into user(id, name) select c1, c2 from music"
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "insert into `user`(id, `name`) select c1, c2 from music",
		BindVariables: map[string]*querypb.BindVariable{},
	}}

	for _, workload := range []string{"olap", "oltp"} {
		sbclookup.Queries = nil
		wQuery := fmt.Sprintf("set @@workload = %s", workload)
		_, err := executor.Execute(context.Background(), "TestInsertSelect", session, wQuery, nil)
		require.NoError(t, err)

		sbclookup.Queries = nil
		_, err = executor.Execute(context.Background(), "TestInsertSelect", session, query, nil)
		require.NoError(t, err)

		assertQueries(t, sbclookup, wantQueries)

		testQueryLog(t, logChan, "TestInsertSelect", "SET", wQuery, 0)
		testQueryLog(t, logChan, "TestInsertSelect", "INSERT", "insert into user(id, name) select c1, c2 from music", 1) // 8 from select and 1 from insert.
	}
}
