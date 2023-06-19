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
	"os"
	"testing"

	_flag "vitess.io/vitess/go/internal/flag"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestSelectNext(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	query := "select next :n values from user_seq"
	bv := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}}

	// Autocommit
	session := NewAutocommitSession(&vtgatepb.Session{})
	_, err := executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Get())
	assert.Zero(t, sbclookup.ReserveCount.Get())
	sbclookup.Queries = nil

	// Txn
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InTransaction = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.EqualValues(t, 1, sbclookup.BeginCount.Get())
	assert.Zero(t, sbclookup.ReserveCount.Get())
	sbclookup.Queries = nil

	// Reserve
	sbclookup.BeginCount.Set(0)
	sbclookup.ReserveCount.Set(0)
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InReservedConn = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Get())
	assert.EqualValues(t, 1, sbclookup.ReserveCount.Get())
	sbclookup.Queries = nil

	// Reserve and Txn
	sbclookup.BeginCount.Set(0)
	sbclookup.ReserveCount.Set(0)
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InReservedConn = true
	session.Session.InTransaction = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.EqualValues(t, 1, sbclookup.BeginCount.Get())
	assert.EqualValues(t, 1, sbclookup.ReserveCount.Get())
}

func _TestSetSystemVariables(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	executor.normalize = true

	sqlparser.SetParserVersion("80001")

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestDefaultShard, SystemVariables: map[string]string{}})

	// Set @@sql_mode and execute a select statement. We should have SET_VAR in the select statement

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})
	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries := []*querypb.BoundQuery{
		{Sql: "set @@sql_mode = 'only_full_group_by'"},
		{Sql: "select :vtg1 from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @var = @@sql_mode", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@sql_mode from dual"},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["var"].GetValue()))
	lookup.Queries = nil

	// Execute a select with a comment that needs a query hint

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select /* comment */ 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select /* comment */ :vtg1 from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "sql_safe_updates", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("0"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_safe_updates = 0", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "set @@sql_safe_updates = 0"},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @var = @@sql_mode", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@sql_mode from dual"},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["var"].GetValue()))
	lookup.Queries = nil

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "max_tmp_tables", Type: sqltypes.VarChar},
			{Name: "sql_mode", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("4"),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @x = @@sql_mode, @y = @@max_tmp_tables", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@sql_mode,@@max_tmp_tables from dual", BindVariables: map[string]*querypb.BindVariable{}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)

	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["x"].GetValue()))
	require.Equal(t, "4", string(session.UserDefinedVariables["y"].GetValue()))
	lookup.Queries = nil

	// Set system variable that is not supported by SET_VAR
	// We expect the next select to not have any SET_VAR query hint, instead it will use set statements

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "max_tmp_tables", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("1"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @@max_tmp_tables = 1", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	wantQueries = []*querypb.BoundQuery{
		{Sql: "select 1 from dual where @@max_tmp_tables != 1"},
		{Sql: "set max_tmp_tables = '1', sql_mode = 'only_full_group_by', sql_safe_updates = '0'", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
		{Sql: "select :vtg1 from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func _TestSetSystemVariablesWithReservedConnection(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, SystemVariables: map[string]string{}})

	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("only_full_group_by"),
			sqltypes.NewVarChar(""),
		}},
	}})
	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = ''", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select age, city from user group by age", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries := []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, '' new"},
		{Sql: "set sql_mode = ''"},
		{Sql: "select age, city, weight_string(age) from `user` group by age, weight_string(age) order by age asc"},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select age, city+1 from user group by age", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, '' new"},
		{Sql: "set sql_mode = ''"},
		{Sql: "select age, city, weight_string(age) from `user` group by age, weight_string(age) order by age asc"},
		{Sql: "select age, city + :vtg1, weight_string(age) from `user` group by age, weight_string(age) order by age asc", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Equal(t, "''", session.SystemVariables["sql_mode"])
	sbc1.Queries = nil
}

func TestCreateTableValidTimestamp(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor", SystemVariables: map[string]string{"sql_mode": "ALLOW_INVALID_DATES"}})

	query := "create table aa(t timestamp default 0)"
	_, err := executor.Execute(context.Background(), "TestSelect", session, query, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	wantQueries := []*querypb.BoundQuery{
		{Sql: "set sql_mode = ALLOW_INVALID_DATES", BindVariables: map[string]*querypb.BindVariable{}},
		{Sql: "create table aa(t timestamp default 0)", BindVariables: map[string]*querypb.BindVariable{}},
	}

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from music_user_map where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestUnshardedComments(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "/* leading */ select id from music_user_map where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)

	_, err = executorExec(executor, "update music_user_map set id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update music_user_map set id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from music_user_map /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1) /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "insert into music_user_map values (1) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestStreamUnsharded(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from music_user_map where id = 1"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		diff := cmp.Diff(wantResult, result)
		t.Errorf("result: %+v, want %+v\ndiff: %s", result, wantResult, diff)
	}
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", sql, 1)
}

func TestStreamBuffering(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// This test is similar to TestStreamUnsharded except that it returns a Result > 10 bytes,
	// such that the splitting of the Result into multiple Result responses gets tested.
	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}, {
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}})

	var results []*sqltypes.Result
	err := executor.StreamExecute(
		context.Background(),
		"TestStreamBuffering",
		NewSafeSession(primarySession),
		"select id from music_user_map where id = 1",
		nil,
		func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		},
	)
	require.NoError(t, err)
	wantResults := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}}
	utils.MustMatch(t, wantResults, results)
}

func TestSelectLastInsertId(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	primarySession.LastInsertId = 52
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select last_insert_id()"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "last_insert_id()", Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewUint64(52),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectSystemVariables(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	primarySession.ReadAfterWrite = &vtgatepb.ReadAfterWrite{
		ReadAfterWriteGtid:    "a fine gtid",
		ReadAfterWriteTimeout: 13,
		SessionTrackGtids:     true,
	}
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select @@autocommit, @@client_found_rows, @@skip_query_plan_cache, @@enable_system_settings, " +
		"@@sql_select_limit, @@transaction_mode, @@workload, @@read_after_write_gtid, " +
		"@@read_after_write_timeout, @@session_track_gtids, @@ddl_strategy, @@socket, @@query_timeout"

	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@@autocommit", Type: sqltypes.Int64},
			{Name: "@@client_found_rows", Type: sqltypes.Int64},
			{Name: "@@skip_query_plan_cache", Type: sqltypes.Int64},
			{Name: "@@enable_system_settings", Type: sqltypes.Int64},
			{Name: "@@sql_select_limit", Type: sqltypes.Int64},
			{Name: "@@transaction_mode", Type: sqltypes.VarChar},
			{Name: "@@`workload`", Type: sqltypes.VarChar},
			{Name: "@@read_after_write_gtid", Type: sqltypes.VarChar},
			{Name: "@@read_after_write_timeout", Type: sqltypes.Float64},
			{Name: "@@session_track_gtids", Type: sqltypes.VarChar},
			{Name: "@@ddl_strategy", Type: sqltypes.VarChar},
			{Name: "@@socket", Type: sqltypes.VarChar},
			{Name: "@@query_timeout", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			// the following are the uninitialised session values
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewVarChar("UNSPECIFIED"),
			sqltypes.NewVarChar(""),
			// these have been set at the beginning of the test
			sqltypes.NewVarChar("a fine gtid"),
			sqltypes.NewFloat64(13),
			sqltypes.NewVarChar("own_gtid"),
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar(""),
			sqltypes.NewInt64(0),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectInitializedVitessAwareVariable(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	primarySession.Autocommit = true
	primarySession.EnableSystemSettings = true
	primarySession.QueryTimeout = 75

	defer func() {
		primarySession.Autocommit = false
		primarySession.EnableSystemSettings = false
		primarySession.QueryTimeout = 0
	}()

	sql := "select @@autocommit, @@enable_system_settings, @@query_timeout"

	result, err := executorExec(executor, sql, nil)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@@autocommit", Type: sqltypes.Int64},
			{Name: "@@enable_system_settings", Type: sqltypes.Int64},
			{Name: "@@query_timeout", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(75),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectUserDefinedVariable(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select @foo"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@foo", Type: sqltypes.Null},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NULL,
		}},
	}
	utils.MustMatch(t, wantResult, result, "Mismatch")

	primarySession = &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []any{"bar"})}
	result, err = executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@foo", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("bar"),
		}},
	}
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestFoundRows(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// run this extra query so we can assert on the number of rows found
	_, err := executorExec(executor, "select 42", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	sql := "select found_rows()"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "found_rows()", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestRowCount(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "select 42", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testRowCount(t, executor, -1)

	_, err = executorExec(executor, "delete from user where id in (42, 24)", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testRowCount(t, executor, 1)
}

func testRowCount(t *testing.T, executor *Executor, wantRowCount int64) {
	t.Helper()
	result, err := executorExec(executor, "select row_count()", map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "row_count()", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(wantRowCount),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectLastInsertIdInUnion(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true
	primarySession.LastInsertId = 52

	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(52),
		}},
	}}
	sbclookup.SetResults(result1)

	sql := "select last_insert_id() as id union select last_insert_id() as id"
	got, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(52),
		}},
	}
	utils.MustMatch(t, wantResult, got, "mismatch")
}

func TestSelectLastInsertIdInWhere(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from music_user_map where id = last_insert_id()"
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = :__lastInsertId",
		BindVariables: map[string]*querypb.BindVariable{"__lastInsertId": sqltypes.Uint64BindVariable(0)},
	}}

	assert.Equal(t, wantQueries, lookup.Queries)
}

func TestLastInsertIDInVirtualTable(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbclookup.SetResults(result1)
	_, err := executorExec(executor, "select * from (select last_insert_id()) as t", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select * from (select :__lastInsertId as `last_insert_id()` from dual) as t",
		BindVariables: map[string]*querypb.BindVariable{"__lastInsertId": sqltypes.Uint64BindVariable(0)},
	}}

	assert.Equal(t, wantQueries, sbclookup.Queries)
}

func TestLastInsertIDInSubQueryExpression(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	primarySession.LastInsertId = 12345
	defer func() {
		// clean up global state
		primarySession.LastInsertId = 0
	}()
	rs, err := executorExec(executor, "select (select last_insert_id()) as x", nil)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "x", Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewUint64(12345),
		}},
	}
	utils.MustMatch(t, rs, wantResult, "Mismatch")

	// the query will get rewritten into a simpler query that can be run entirely on the vtgate
	assert.Empty(t, sbc1.Queries)
	assert.Empty(t, sbc2.Queries)
}

func TestSelectDatabase(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	sql := "select database()"
	newSession := proto.Clone(primarySession).(*vtgatepb.Session)
	session := NewSafeSession(newSession)
	session.TargetString = "TestExecutor@primary"
	result, err := executor.Execute(
		context.Background(),
		"TestExecute",
		session,
		sql,
		map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "database()", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("TestExecutor@primary"),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")

}

func TestSelectINFromOR(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	_, err := executorExec(executor, "select 1 from user where id = 1 and name = 'apa' or id = 2 and name = 'toto'", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select 1 from `user` where id = 1 and `name` = 'apa' or id = 2 and `name` = 'toto'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestSelectDual(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()

	_, err := executorExec(executor, "select @@aa.bb from dual", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select @@`aa.bb` from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestSelectComments(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()

	_, err := executorExec(executor, "/* leading */ select id from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from `user` where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestSelectNormalize(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	executor.normalize = true

	_, err := executorExec(executor, "/* leading */ select id from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ select id from `user` where id = :id /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil
}

func TestSelectCaseSensitivity(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()

	_, err := executorExec(executor, "select Id from user where iD = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id from `user` where iD = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestStreamSelectEqual(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	sql := "select id from user where id = 1"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestStreamSelectIN(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	sql := "select id from user where id in (1)"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where name = 'foo'"
	result, err = executorStream(executor, sql)
	require.NoError(t, err)
	wantResult = sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id in (1)",
		BindVariables: map[string]*querypb.BindVariable{},
	},
		{
			Sql:           "select id from `user` where `name` = 'foo'",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func createExecutor(serv *sandboxTopo, cell string, resolver *Resolver) *Executor {
	return NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_V3)
}

// TODO(sougou): stream and non-stream testing are very similar.
// Could reuse code,
func TestSimpleJoin(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3"
	_, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u2.id from `user` as u1 join `user` as u2 where u1.id = 1 and u2.id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestJoinComments(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3 /* trailing */"
	_, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u2.id from `user` as u1 join `user` as u2 where u1.id = 1 and u2.id = 3 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	testQueryLog(t, logChan, "TestExecute", "SELECT", sql, 1)
}

func TestSelectDatabasePrepare(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select database()"
	_, err := executorPrepare(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
}

func TestSelectLock(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	session := NewSafeSession(nil)
	session.Session.InTransaction = true
	session.ShardSessions = []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace:   "TestExecutor",
			Shard:      "-20",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		TransactionId: 12345,
		TabletAlias:   sbc1.Tablet().Alias,
	}}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select get_lock('lock name', 10) from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestExecutor",
				Shard:      "-20",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 12345,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
		LockSession: &vtgatepb.Session_ShardSession{
			Target:      &querypb.Target{Keyspace: "TestExecutor", Shard: "-20", TabletType: topodatapb.TabletType_PRIMARY},
			TabletAlias: sbc1.Tablet().Alias,
			ReservedId:  1,
		},
		AdvisoryLock: map[string]int64{"lock name": 1},
		FoundRows:    1,
		RowCount:     -1,
	}

	_, err := exec(executor, session, "select get_lock('lock name', 10) from dual")
	require.NoError(t, err)
	wantSession.LastLockHeartbeat = session.Session.LastLockHeartbeat // copying as this is current timestamp value.
	utils.MustMatch(t, wantSession, session.Session, "")
	utils.MustMatch(t, wantQueries, sbc1.Queries, "")

	wantQueries = append(wantQueries, &querypb.BoundQuery{
		Sql:           "select release_lock('lock name') from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	wantSession.AdvisoryLock = nil
	wantSession.LockSession = nil

	_, err = exec(executor, session, "select release_lock('lock name') from dual")
	require.NoError(t, err)
	wantSession.LastLockHeartbeat = session.Session.LastLockHeartbeat // copying as this is current timestamp value.
	utils.MustMatch(t, wantQueries, sbc1.Queries, "")
	utils.MustMatch(t, wantSession, session.Session, "")
}

func TestLockReserve(t *testing.T) {
	// no connection should be reserved for these queries.
	tcases := []string{
		"select is_free_lock('lock name') from dual",
		"select is_used_lock('lock name') from dual",
		"select release_all_locks() from dual",
		"select release_lock('lock name') from dual",
	}

	executor, _, _, _ := createExecutorEnv()
	session := NewAutocommitSession(&vtgatepb.Session{})

	for _, sql := range tcases {
		t.Run(sql, func(t *testing.T) {
			_, err := exec(executor, session, sql)
			require.NoError(t, err)
			require.Nil(t, session.LockSession)
		})
	}

	// get_lock should reserve a connection.
	_, err := exec(executor, session, "select get_lock('lock name', 10) from dual")
	require.NoError(t, err)
	require.NotNil(t, session.LockSession)

}

func TestGen4SelectStraightJoin(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select u.id from user u straight_join user2 u2 on u.id = u2.id"
	_, err := executor.Execute(context.Background(),
		"TestGen4SelectStraightJoin",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select u.id from `user` as u straight_join user2 as u2 on u.id = u2.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestGen4MultiColMultiEqual(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where (cola,colb) in ((17984,2),(17984,3))"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColMultiEqual",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where (cola, colb) in ((:vtg1, :vtg2), (:vtg1, :vtg3))",
			BindVariables: map[string]*querypb.BindVariable{
				"vtg1": sqltypes.Int64BindVariable(17984),
				"vtg2": sqltypes.Int64BindVariable(2),
				"vtg3": sqltypes.Int64BindVariable(3),
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestGen4SelectUnqualifiedReferenceTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select * from zip_detail"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           query,
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	require.Nil(t, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestGen4SelectQualifiedReferenceTable(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := fmt.Sprintf("select * from %s.zip_detail", KsTestDefaultShard)
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select * from _vt.zip_detail",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestGen4JoinUnqualifiedReferenceTable(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select * from user join zip_detail on user.zip_detail_id = zip_detail.id"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select * from `user` join zip_detail on `user`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)

	sbclookup.Queries = nil
	query = "select * from simple join zip_detail on simple.zip_detail_id = zip_detail.id"
	_, err = executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select * from `simple` join zip_detail on `simple`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestGen4CrossShardJoinQualifiedReferenceTable(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select simple.id from simple join TestExecutor.zip_detail on simple.zip_detail_id = TestExecutor.zip_detail.id"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	unshardedWantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select `simple`.id from `simple` join TestExecutor.zip_detail on `simple`.zip_detail_id = TestExecutor.zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, unshardedWantQueries, sbclookup.Queries)
}

var multiColVschema = `
{
	"sharded": true,
	"vindexes": {
		"multicol_vdx": {
			"type": "multicol",
			"params": {
				"column_count": "3",
				"column_bytes": "1,3,4",
				"column_vindex": "hash,binary,unicode_loose_xxhash"
			}
        }
	},
	"tables": {
		"multicoltbl": {
			"column_vindexes": [
				{
					"columns": ["cola","colb","colc"],
					"name": "multicol_vdx"
				}
			]
		}
	}
}
`

func TestSelectHexAndBit(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	qr, err := executor.Execute(context.Background(), "TestSelectHexAndBit", session,
		"select 0b1001, b'1001', 0x9, x'09'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\t") VARBINARY("\t")]]`, fmt.Sprintf("%v", qr.Rows))

	qr, err = executor.Execute(context.Background(), "TestSelectHexAndBit", session,
		"select 1 + 0b1001, 1 + b'1001', 1 + 0x9, 1 + x'09'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[UINT64(10) UINT64(10) UINT64(10) UINT64(10)]]`, fmt.Sprintf("%v", qr.Rows))
}

// TestSelectCFC tests validates that cfc vindex plan gets cached and same plan is getting reused.
// This also validates that cache_size is able to calculate the cfc vindex plan size.
func TestSelectCFC(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	for i := 1; i < 100; i++ {
		_, err := executor.Execute(context.Background(), "TestSelectCFC", session,
			"select /*vt+ PLANNER=gen4 */ c2 from tbl_cfc where c1 like 'A%'", nil)
		require.NoError(t, err)
		assert.EqualValues(t, 1, executor.plans.Misses(), "missed count:")
		assert.EqualValues(t, i-1, executor.plans.Hits(), "hit count:")
	}
}

func TestSelectView(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	// add the view to local vschema
	err := executor.vschema.AddView(KsTestDefaultShard, "user_details_view", "select user.id, user_extra.col from user join user_extra on user.id = user_extra.user_id")
	require.NoError(t, err)

	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)

	sbclookup.Queries = nil
	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view where id = 2", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view where id = :id",
		BindVariables: map[string]*querypb.BindVariable{
			"id": sqltypes.Int64BindVariable(2),
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)

	sbclookup.Queries = nil
	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view where id in (1,2,3,4,5)", nil)
	require.NoError(t, err)
	bvtg1, _ := sqltypes.BuildBindVariable([]int64{1, 2, 3, 4, 5})
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view where id in ::vtg1",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1": bvtg1,
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}
