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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/vt/topo"

	"github.com/google/go-cmp/cmp"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestExecutorResultsExceeded(t *testing.T) {
	save := warnMemoryRows
	warnMemoryRows = 3
	defer func() { warnMemoryRows = save }()

	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	initial := warnings.Counts()["ResultsExceeded"]

	result1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1")
	result2 := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	sbclookup.SetResults([]*sqltypes.Result{result1, result2})

	_, err := executor.Execute(ctx, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial, warnings.Counts()["ResultsExceeded"], "warnings count")

	_, err = executor.Execute(ctx, "TestExecutorResultsExceeded", session, "select * from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, initial+1, warnings.Counts()["ResultsExceeded"], "warnings count")
}

func TestExecutorMaxMemoryRowsExceeded(t *testing.T) {
	save := maxMemoryRows
	maxMemoryRows = 3
	defer func() { maxMemoryRows = save }()

	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64"), "1", "2", "3", "4")
	fn := func(r *sqltypes.Result) error {
		return nil
	}
	testCases := []struct {
		query string
		err   string
	}{
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from main1", ""},
		{"select * from main1", "in-memory row count exceeded allowed limit of 3"},
	}

	for _, test := range testCases {
		sbclookup.SetResults([]*sqltypes.Result{result})
		stmt, err := sqlparser.Parse(test.query)
		require.NoError(t, err)

		_, err = executor.Execute(ctx, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil)
		if sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt) {
			require.NoError(t, err, "no error when DirectiveIgnoreMaxMemoryRows is provided")
		} else {
			assert.EqualError(t, err, test.err, "maxMemoryRows limit exceeded")
		}

		sbclookup.SetResults([]*sqltypes.Result{result})
		err = executor.StreamExecute(ctx, "TestExecutorMaxMemoryRowsExceeded", session, test.query, nil, fn)
		require.NoError(t, err, "maxMemoryRows limit does not apply to StreamExecute")
	}
}

func TestExecutorTransactionsNoAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@primary", SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 0, sbclookup.CommitCount.Get(), "commit count")
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime, "logstats: expected zero CommitTime")
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	_, err = executor.Execute(context.Background(), "TestExecute", session, "commit", nil)
	if err != nil {
		t.Fatal(err)
	}
	wantSession = &vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("begin: %v, want %v", session.Session, wantSession)
	}
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.RollbackCount.Get(), "rollback count")
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	if logStats.CommitTime == 0 {
		t.Errorf("logstats: expected non-zero CommitTime")
	}
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// CloseSession doesn't log anything
	err = executor.CloseSession(ctx, session)
	require.NoError(t, err)
	logStats = getQueryLog(logChan)
	if logStats != nil {
		t.Errorf("logstats: expected no record for no-op rollback, got %v", logStats)
	}

	// Prevent use of non-primary if in_transaction is on.
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@primary", InTransaction: true})
	_, err = executor.Execute(ctx, "TestExecute", session, "use @replica", nil)
	require.EqualError(t, err, `can't execute the given command because you have an active transaction`)
}

func TestDirectTargetRewrites(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	executor.normalize = true

	session := &vtgatepb.Session{
		TargetString:    "TestUnsharded/0@primary",
		Autocommit:      true,
		TransactionMode: vtgatepb.TransactionMode_MULTI,
	}
	sql := "select database()"

	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(session), sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	assertQueries(t, sbclookup, []*querypb.BoundQuery{{
		Sql:           "select :__vtdbname as `database()` from dual",
		BindVariables: map[string]*querypb.BindVariable{"__vtdbname": sqltypes.StringBindVariable("TestUnsharded/0@primary")},
	}})
}

func TestExecutorTransactionsAutoCommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// begin.
	_, err := executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{InTransaction: true, TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if commitCount := sbclookup.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.CommitCount.Get())

	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	assert.NotEqual(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	if rollbackCount := sbclookup.RollbackCount.Get(); rollbackCount != 1 {
		t.Errorf("want 1, got %d", rollbackCount)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	_ = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	logStats = testQueryLog(t, logChan, "TestExecute", "ROLLBACK", "rollback", 1)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
}

func TestExecutorTransactionsAutoCommitStreaming(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	oltpOptions := &querypb.ExecuteOptions{Workload: querypb.ExecuteOptions_OLTP}
	session := NewSafeSession(&vtgatepb.Session{
		TargetString: "@primary",
		Autocommit:   true,
		Options:      oltpOptions,
		SessionUUID:  "suuid",
	})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	var results []*sqltypes.Result

	// begin.
	err := executor.StreamExecute(ctx, "TestExecute", session, "begin", nil, func(result *sqltypes.Result) error {
		results = append(results, result)
		return nil
	})

	require.EqualValues(t, 1, len(results), "should get empty result from begin")
	assert.Empty(t, results[0].Rows, "should get empty result from begin")

	require.NoError(t, err)
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		TargetString:  "@primary",
		Autocommit:    true,
		Options:       oltpOptions,
		SessionUUID:   "suuid",
	}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.Zero(t, sbclookup.CommitCount.Get())
	logStats := testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// commit.
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, Options: oltpOptions, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.CommitCount.Get())

	logStats = testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	assert.EqualValues(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")
	logStats = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)
	assert.NotEqual(t, 0, logStats.CommitTime)
	assert.EqualValues(t, "suuid", logStats.SessionUUID, "logstats: expected non-empty SessionUUID")

	// rollback.
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "rollback", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{TargetString: "@primary", Autocommit: true, Options: oltpOptions, SessionUUID: "suuid"}
	utils.MustMatch(t, wantSession, session.Session, "session")
	assert.EqualValues(t, 1, sbclookup.RollbackCount.Get())
}

func TestExecutorDeleteMetadata(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		vschemaacl.AuthorizedDDLUsers = ""
	}()

	executor, _, _, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary", Autocommit: true})

	set := "set @@vitess_metadata.app_v1= '1'"
	_, err := executor.Execute(ctx, "TestExecute", session, set, nil)
	assert.NoError(t, err, "%s error: %v", set, err)

	show := `show vitess_metadata variables like 'app\\_%'`
	result, _ := executor.Execute(ctx, "TestExecute", session, show, nil)
	assert.Len(t, result.Rows, 1)

	// Fails if deleting key that doesn't exist
	delQuery := "set @@vitess_metadata.doesn't_exist=''"
	_, err = executor.Execute(ctx, "TestExecute", session, delQuery, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))

	// Delete existing key, show should fail given the node doesn't exist
	delQuery = "set @@vitess_metadata.app_v1=''"
	_, err = executor.Execute(ctx, "TestExecute", session, delQuery, nil)
	assert.NoError(t, err)

	show = `show vitess_metadata variables like 'app\\_%'`
	_, err = executor.Execute(ctx, "TestExecute", session, show, nil)
	assert.True(t, topo.IsErrType(err, topo.NoNode))
}

func TestExecutorAutocommit(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})

	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// autocommit = 0
	startCount := sbclookup.CommitCount.Get()
	_, err := executor.Execute(ctx, "TestExecute", session, "select id from main1", nil)
	require.NoError(t, err)
	wantSession := &vtgatepb.Session{TargetString: "@primary", InTransaction: true, FoundRows: 1, RowCount: -1}
	testSession := proto.Clone(session.Session).(*vtgatepb.Session)
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, testSession, "session does not match for autocommit=0")

	logStats := testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from main1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsReturned == 0 {
		t.Errorf("logstats: expected non-zero RowsReturned")
	}

	// autocommit = 1
	_, err = executor.Execute(ctx, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, logChan, "TestExecute", "SET", "set @@autocommit = 1", 0)

	// Setting autocommit=1 commits existing transaction.
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary", FoundRows: 0, RowCount: 1}
	utils.MustMatch(t, wantSession, session.Session, "session does not match for autocommit=1")

	logStats = testQueryLog(t, logChan, "TestExecute", "UPDATE", "update main1 set id=1", 1)
	assert.NotZero(t, logStats.CommitTime, "logstats: expected non-zero CommitTime")
	assert.NotEqual(t, uint64(0), logStats.RowsAffected, "logstats: expected non-zero RowsAffected")

	// autocommit = 1, "begin"
	session.ResetTx()
	startCount = sbclookup.CommitCount.Get()
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_ = testQueryLog(t, logChan, "TestExecute", "BEGIN", "begin", 0)

	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{InTransaction: true, Autocommit: true, TargetString: "@primary", FoundRows: 0, RowCount: 1}
	testSession = proto.Clone(session.Session).(*vtgatepb.Session)
	testSession.ShardSessions = nil
	utils.MustMatch(t, wantSession, testSession, "session does not match for autocommit=1")
	if got, want := sbclookup.CommitCount.Get(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}

	logStats = testQueryLog(t, logChan, "TestExecute", "UPDATE", "update main1 set id=1", 1)
	if logStats.CommitTime != 0 {
		t.Errorf("logstats: expected zero CommitTime")
	}
	if logStats.RowsAffected == 0 {
		t.Errorf("logstats: expected non-zero RowsAffected")
	}

	_, err = executor.Execute(ctx, "TestExecute", session, "commit", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_ = testQueryLog(t, logChan, "TestExecute", "COMMIT", "commit", 1)

	// transition autocommit from 0 to 1 in the middle of a transaction.
	startCount = sbclookup.CommitCount.Get()
	session = NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	_, err = executor.Execute(ctx, "TestExecute", session, "begin", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "update main1 set id=1", nil)
	require.NoError(t, err)
	if got, want := sbclookup.CommitCount.Get(), startCount; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
	_, err = executor.Execute(ctx, "TestExecute", session, "set autocommit=1", nil)
	require.NoError(t, err)
	wantSession = &vtgatepb.Session{Autocommit: true, TargetString: "@primary"}
	if !proto.Equal(session.Session, wantSession) {
		t.Errorf("autocommit=1: %v, want %v", session.Session, wantSession)
	}
	if got, want := sbclookup.CommitCount.Get(), startCount+1; got != want {
		t.Errorf("Commit count: %d, want %d", got, want)
	}
}

func sortString(w string) string {
	s := strings.Split(w, "")
	sort.Strings(s)
	return strings.Join(s, "")
}

func assertMatchesNoOrder(t *testing.T, expected, got string) {
	t.Helper()
	if sortString(expected) != sortString(got) {
		t.Errorf("for query: expected \n%s \nbut actual \n%s", expected, got)
	}
}

func TestExecutorShow(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})

	for _, query := range []string{"show vitess_keyspaces", "show keyspaces"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		assertMatchesNoOrder(t, `[[VARCHAR("TestExecutor")] [VARCHAR("TestUnsharded")] [VARCHAR("TestXBadSharding")] [VARCHAR("TestXBadVSchema")]]`, fmt.Sprintf("%v", qr.Rows))
	}

	for _, query := range []string{"show databases", "show DATABASES", "show schemas", "show SCHEMAS"} {
		qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
		require.NoError(t, err)
		// Showing default tables (5+4[default])
		assertMatchesNoOrder(t, `[[INT32(1) VARCHAR("foo")]]`, fmt.Sprintf("%v", qr.Rows))
	}

	_, err := executor.Execute(ctx, "TestExecute", session, "show variables", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "show collation", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "TestExecute", session, "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'", nil)
	require.NoError(t, err)

	showResults := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Tables_in_keyspace", Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("some_table"),
		}},
	}
	sbclookup.SetResults([]*sqltypes.Result{showResults})

	query := fmt.Sprintf("show tables from %v", KsTestUnsharded)
	qr, err := executor.Execute(ctx, "TestExecute", session, query, nil)
	require.NoError(t, err)

	lastQuery := sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	want := "show tables from TestUnsharded"
	assert.Equal(t, want, lastQuery, "Got: %v, want %v", lastQuery, want)

	wantqr := showResults
	utils.MustMatch(t, wantqr, qr, fmt.Sprintf("unexpected results running query: %s", query))

	// SHOW CREATE table using vschema to find keyspace.
	_, err = executor.Execute(ctx, "TestExecute", session, "show create table user_seq", nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery := "show create table user_seq"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW CREATE table with query-provided keyspace
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show create table %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show create table TestUnsharded.unknown"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW KEYS with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show keys from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show indexes from TestUnsharded.unknown"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	wantQuery = "show indexes from unknown from TestUnsharded"
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show keys from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	wantQuery = "show indexes from TestUnsharded.unknown"
	// SHOW INDEX with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show index from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	wantQuery = "show indexes from unknown from TestUnsharded"
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show index from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	wantQuery = "show indexes from TestUnsharded.unknown"
	// SHOW INDEXES with two different syntax
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show indexes from %v.unknown", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show indexes from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	wantQuery = "show indexes from unknown from TestUnsharded"
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, wantQuery)

	// SHOW EXTENDED {INDEX | INDEXES | KEYS}
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended index from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, "show indexes from unknown from TestUnsharded")

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended indexes from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, "show indexes from unknown from TestUnsharded")

	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show extended keys from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
	lastQuery = sbclookup.Queries[len(sbclookup.Queries)-1].Sql
	assert.Equal(t, wantQuery, lastQuery, "Got: %v. Want: %v", lastQuery, "show indexes from unknown from TestUnsharded")

	// Set desitation keyspace in session
	session.TargetString = KsTestUnsharded
	_, err = executor.Execute(ctx, "TestExecute", session, "show create table unknown", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "TestExecute", session, "show full columns from table1", nil)
	require.NoError(t, err)

	// Reset target string so other tests dont fail.
	session.TargetString = "@primary"
	_, err = executor.Execute(ctx, "TestExecute", session, fmt.Sprintf("show full columns from unknown from %v", KsTestUnsharded), nil)
	require.NoError(t, err)
}

func TestExecutorShowTargeted(t *testing.T) {
	executor, _, sbc2, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor/40-60"})

	queries := []string{
		"show databases",
		"show variables like 'read_only'",
		"show collation",
		"show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
		"show tables",
		fmt.Sprintf("show tables from %v", KsTestUnsharded),
		"show create table user_seq",
		"show full columns from table1",
		"show plugins",
		"show warnings",
	}

	for _, sql := range queries {
		_, err := executor.Execute(ctx, "TestExecutorShowTargeted", session, sql, nil)
		require.NoError(t, err)
		assert.NotZero(t, len(sbc2.Queries), "Tablet should have received 'show' query")
		lastQuery := sbc2.Queries[len(sbc2.Queries)-1].Sql
		assert.Equal(t, sql, lastQuery, "Got: %v, want %v", lastQuery, sql)
	}
}

func TestExecutorUse(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{Autocommit: true, TargetString: "@primary"})

	stmts := []string{
		"use TestExecutor",
		"use `TestExecutor:-80@primary`",
	}
	want := []string{
		"TestExecutor",
		"TestExecutor:-80@primary",
	}
	for i, stmt := range stmts {
		_, err := executor.Execute(ctx, "TestExecute", session, stmt, nil)
		if err != nil {
			t.Error(err)
		}
		wantSession := &vtgatepb.Session{Autocommit: true, TargetString: want[i], RowCount: -1}
		utils.MustMatch(t, wantSession, session.Session, "session does not match")
	}

	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use 1", nil)
	wantErr := "syntax error at position 6 near '1'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got: %v, want %v", err, wantErr)
	}

	_, err = executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "use UnexistentKeyspace", nil)
	require.EqualError(t, err, "VT05003: unknown database 'UnexistentKeyspace' in vschema")
}

func TestExecutorComment(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	stmts := []string{
		"/*! SET autocommit=1*/",
		"/*!50708 SET @x=5000*/",
	}
	wantResult := &sqltypes.Result{}

	for _, stmt := range stmts {
		gotResult, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
		if err != nil {
			t.Error(err)
		}
		if !gotResult.Equal(wantResult) {
			t.Errorf("Exec %s: %v, want %v", stmt, gotResult, wantResult)
		}
	}
}

func _TestExecutorOther(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr       bool
		hasDestinationShardErr bool
		wantCnts               cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:              "TestExecutor[-]",
			hasDestinationShardErr: true,
		},
		{
			targetStr: KsTestUnsharded,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr: "TestExecutor",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr: "TestExecutor/-20",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr: "TestExecutor[00]",
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"analyze table t1",
		"describe select * from t1",
		"explain select * from t1",
		"repair table t1",
		"optimize table t1",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			t.Run(fmt.Sprintf("%s-%s", stmt, tc.targetStr), func(t *testing.T) {
				sbc1.ExecCount.Set(0)
				sbc2.ExecCount.Set(0)
				sbclookup.ExecCount.Set(0)

				_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
				if tc.hasNoKeyspaceErr {
					assert.Error(t, err, errNoKeyspace)
				} else if tc.hasDestinationShardErr {
					assert.Errorf(t, err, "Destination can only be a single shard for statement: %s", stmt)
				} else {
					assert.NoError(t, err)
				}

				utils.MustMatch(t, tc.wantCnts, cnts{
					Sbc1Cnt:      sbc1.ExecCount.Get(),
					Sbc2Cnt:      sbc2.ExecCount.Get(),
					SbcLookupCnt: sbclookup.ExecCount.Get(),
				})
			})
		}
	}
}

func _TestExecutorDDL(t *testing.T) {
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr bool
		shardQueryCnt    int
		wantCnts         cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:     KsTestUnsharded,
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr:     "TestExecutor",
			shardQueryCnt: 8,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      1,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr:     "TestExecutor/-20",
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"create table t2(id bigint primary key)",
		"alter table t2 add primary key (id)",
		"rename table t2 to t3",
		"truncate table t2",
		"drop table t2",
		`create table test_partitioned (
			id bigint,
			date_create int,		
			primary key(id)
		) Engine=InnoDB	/*!50100 PARTITION BY RANGE (date_create)
		  (PARTITION p2018_06_14 VALUES LESS THAN (1528959600) ENGINE = InnoDB,
		   PARTITION p2018_06_15 VALUES LESS THAN (1529046000) ENGINE = InnoDB,
		   PARTITION p2018_06_16 VALUES LESS THAN (1529132400) ENGINE = InnoDB,
		   PARTITION p2018_06_17 VALUES LESS THAN (1529218800) ENGINE = InnoDB)*/`,
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			sbc1.ExecCount.Set(0)
			sbc2.ExecCount.Set(0)
			sbclookup.ExecCount.Set(0)
			stmtType := "DDL"
			_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
			if tc.hasNoKeyspaceErr {

			} else {
				require.NoError(t, err, "did not expect error for query: %q", stmt)
			}

			diff := cmp.Diff(tc.wantCnts, cnts{
				Sbc1Cnt:      sbc1.ExecCount.Get(),
				Sbc2Cnt:      sbc2.ExecCount.Get(),
				SbcLookupCnt: sbclookup.ExecCount.Get(),
			})
			if diff != "" {
				t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
			}

			testQueryLog(t, logChan, "TestExecute", stmtType, stmt, tc.shardQueryCnt)
		}
	}

	stmts2 := []struct {
		input  string
		hasErr bool
	}{
		{input: "create table t1(id bigint primary key)", hasErr: false},
		{input: "drop table t1", hasErr: false},
		{input: "drop table t2", hasErr: true},
		{input: "drop view t1", hasErr: false},
		{input: "drop view t2", hasErr: true},
		{input: "alter view t1 as select * from t1", hasErr: false},
		{input: "alter view t2 as select * from t1", hasErr: true},
	}

	for _, stmt := range stmts2 {
		sbc1.ExecCount.Set(0)
		sbc2.ExecCount.Set(0)
		sbclookup.ExecCount.Set(0)
		_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: ""}), stmt.input, nil)
		if stmt.hasErr {
			require.EqualError(t, err, errNoKeyspace.Error(), "expect query to fail")
			testQueryLog(t, logChan, "TestExecute", "", stmt.input, 0)
		} else {
			require.NoError(t, err)
			testQueryLog(t, logChan, "TestExecute", "DDL", stmt.input, 8)
		}
	}
}

func _TestExecutorDDLFk(t *testing.T) {
	executor, _, _, sbc := createExecutorEnv()

	mName := "TestExecutorDDLFk"
	stmts := []string{
		"create table t1(id bigint primary key, foreign key (id) references t2(id))",
		"alter table t2 add foreign key (id) references t1(id) on delete cascade",
	}

	for _, stmt := range stmts {
		for _, fkMode := range []string{"allow", "disallow"} {
			t.Run(stmt+fkMode, func(t *testing.T) {
				sbc.ExecCount.Set(0)
				foreignKeyMode = fkMode
				_, err := executor.Execute(ctx, mName, NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded}), stmt, nil)
				if fkMode == "allow" {
					require.NoError(t, err)
					require.EqualValues(t, 1, sbc.ExecCount.Get())
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "foreign key constraints are not allowed")
				}
			})
		}
	}
}

func TestExecutorUnrecognized(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	_, err := executor.Execute(ctx, "TestExecute", NewSafeSession(&vtgatepb.Session{}), "invalid statement", nil)
	require.Error(t, err, "unrecognized statement: invalid statement'")
}

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _ := createExecutorEnv()

	stats := r.VSchemaStats()

	templ := template.New("")
	templ, err := templ.Parse(VSchemaTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, stats); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
	result := wr.String()
	if !strings.Contains(result, "<td>TestXBadSharding</td>") ||
		!strings.Contains(result, "<td>TestUnsharded</td>") {
		t.Errorf("invalid html result: %v", result)
	}
}

var pv = querypb.ExecuteOptions_Gen4

func assertCacheSize(t *testing.T, c cache.Cache, expected int) {
	t.Helper()
	var size int
	c.ForEach(func(_ any) bool {
		size++
		return true
	})
	if size != expected {
		t.Errorf("getPlan() expected cache to have size %d, but got: %d", expected, size)
	}
}

func assertCacheContains(t *testing.T, e *Executor, want []string) {
	t.Helper()
	for _, wantKey := range want {
		if _, ok := e.debugGetPlan(wantKey); !ok {
			t.Errorf("missing key in plan cache: %v", wantKey)
		}
	}
}

func getPlanCached(t *testing.T, e *Executor, vcursor *vcursorImpl, sql string, comments sqlparser.MarginComments, bindVars map[string]*querypb.BindVariable, skipQueryPlanCache bool) (*engine.Plan, *logstats.LogStats) {
	logStats := logstats.NewLogStats(ctx, "Test", "", "", nil)
	plan, _, err := e.getPlan(context.Background(), vcursor, sql, comments, bindVars, &SafeSession{
		Session: &vtgatepb.Session{Options: &querypb.ExecuteOptions{SkipQueryPlanCache: skipQueryPlanCache}},
	}, logStats)
	require.NoError(t, err)

	// Wait for cache to settle
	e.plans.Wait()
	return plan, logStats
}

func TestGetPlanCacheUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	query1 := "select * from music_user_map where id = 1"

	_, logStats1 := getPlanCached(t, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true)
	assertCacheSize(t, r.plans, 0)

	wantSQL := query1 + " /* comment */"
	if logStats1.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats1.SQL)
	}

	_, logStats2 := getPlanCached(t, r, emptyvc, query1, makeComments(" /* comment 2 */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 1)

	wantSQL = query1 + " /* comment 2 */"
	if logStats2.SQL != wantSQL {
		t.Errorf("logstats sql want \"%s\" got \"%s\"", wantSQL, logStats2.SQL)
	}

	// Skip cache using directive
	r, _, _, _ = createExecutorEnv()
	unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	getPlanCached(t, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 0)

	query1 = "insert into user(id) values (1), (2)"
	getPlanCached(t, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 1)

	// the target string will be resolved and become part of the plan cache key, which adds a new entry
	ksIDVc1, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	getPlanCached(t, r, ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 2)

	// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
	ksIDVc2, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	getPlanCached(t, r, ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 2)
}

func TestGetPlanCacheNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	emptyvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

	query1 := "select * from music_user_map where id = 1"
	_, logStats1 := getPlanCached(t, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, true /* skipQueryPlanCache */)
	assertCacheSize(t, r.plans, 0)
	wantSQL := "select * from music_user_map where id = :id /* comment */"
	assert.Equal(t, wantSQL, logStats1.SQL)

	_, logStats2 := getPlanCached(t, r, emptyvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false /* skipQueryPlanCache */)
	assertCacheSize(t, r.plans, 1)
	assert.Equal(t, wantSQL, logStats2.SQL)

	// Skip cache using directive
	r, _, _, _ = createExecutorEnv()
	r.normalize = true
	unshardedvc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "@unknown"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)

	query1 = "insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)"
	getPlanCached(t, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 0)

	query1 = "insert into user(id) values (1), (2)"
	getPlanCached(t, r, unshardedvc, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 1)

	// the target string will be resolved and become part of the plan cache key, which adds a new entry
	ksIDVc1, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[deadbeef]"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	getPlanCached(t, r, ksIDVc1, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 2)

	// the target string will be resolved and become part of the plan cache key, as it's an unsharded ks, it will be the same entry as above
	ksIDVc2, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded + "[beefdead]"}), "", makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
	getPlanCached(t, r, ksIDVc2, query1, makeComments(" /* comment */"), map[string]*querypb.BindVariable{}, false)
	assertCacheSize(t, r.plans, 2)
}

func TestParseEmptyTargetMultiKeyspace(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	altVSchema := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			KsTestUnsharded: r.vschema.Keyspaces[KsTestUnsharded],
			KsTestSharded:   r.vschema.Keyspaces[KsTestSharded],
		},
	}
	r.vschema = altVSchema

	destKeyspace, destTabletType, _, _ := r.ParseDestinationTarget("")
	if destKeyspace != "" || destTabletType != topodatapb.TabletType_PRIMARY {
		t.Errorf(
			"parseDestinationTarget(%s): got (%v, %v), want (%v, %v)",
			"@primary",
			destKeyspace,
			destTabletType,
			"",
			topodatapb.TabletType_PRIMARY,
		)
	}
}

func TestDebugVSchema(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/debug/vschema", nil)

	executor, _, _, _ := createExecutorEnv()
	executor.ServeHTTP(resp, req)
	v := make(map[string]any)
	if err := json.Unmarshal(resp.Body.Bytes(), &v); err != nil {
		t.Fatalf("Unmarshal on %s failed: %v", resp.Body.String(), err)
	}
	if _, ok := v["routing_rules"]; !ok {
		t.Errorf("routing rules missing: %v", resp.Body.String())
	}
	if _, ok := v["keyspaces"]; !ok {
		t.Errorf("keyspaces missing: %v", resp.Body.String())
	}
}

func TestExecutorMaxPayloadSizeExceeded(t *testing.T) {
	saveMax := maxPayloadSize
	saveWarn := warnPayloadSize
	maxPayloadSize = 10
	warnPayloadSize = 5
	defer func() {
		maxPayloadSize = saveMax
		warnPayloadSize = saveWarn
	}()

	executor, _, _, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
	warningCount := warnings.Counts()["WarnPayloadSizeExceeded"]
	testMaxPayloadSizeExceeded := []string{
		"select * from main1",
		"insert into main1(id) values (1), (2)",
		"update main1 set id=1",
		"delete from main1 where id=1",
	}
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
		require.NotNil(t, err)
		assert.EqualError(t, err, "query payload size above threshold")
	}
	assert.Equal(t, warningCount, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")

	testMaxPayloadSizeOverride := []string{
		"select /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ * from main1",
		"insert /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ into main1(id) values (1), (2)",
		"update /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ main1 set id=1",
		"delete /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ from main1 where id=1",
	}
	for _, query := range testMaxPayloadSizeOverride {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeWithOverride", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")

	maxPayloadSize = 1000
	for _, query := range testMaxPayloadSizeExceeded {
		_, err := executor.Execute(context.Background(), "TestExecutorMaxPayloadSizeExceeded", session, query, nil)
		assert.Equal(t, nil, err, "err should be nil")
	}
	assert.Equal(t, warningCount+4, warnings.Counts()["WarnPayloadSizeExceeded"], "warnings count")
}

func TestOlapSelectDatabase(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true

	session := &vtgatepb.Session{Autocommit: true}

	sql := `select database()`
	cbInvoked := false
	cb := func(r *sqltypes.Result) error {
		cbInvoked = true
		return nil
	}
	err := executor.StreamExecute(context.Background(), "TestExecute", NewSafeSession(session), sql, nil, cb)
	assert.NoError(t, err)
	assert.True(t, cbInvoked)
}

func TestExecutorClearsWarnings(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{
		Warnings: []*querypb.QueryWarning{{Code: 234, Message: "oh noes"}},
	})
	_, err := executor.Execute(context.Background(), "TestExecute", session, "select 42", nil)
	require.NoError(t, err)
	require.Empty(t, session.Warnings)
}

func TestExecutorStartTxnStmt(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	session := NewAutocommitSession(&vtgatepb.Session{})

	tcases := []struct {
		beginSQL        string
		expTxAccessMode []querypb.ExecuteOptions_TransactionAccessMode
	}{{
		beginSQL: "begin",
	}, {
		beginSQL: "start transaction",
	}, {
		beginSQL:        "start transaction with consistent snapshot",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT},
	}, {
		beginSQL:        "start transaction read only",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_ONLY},
	}, {
		beginSQL:        "start transaction read write",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_WRITE},
	}, {
		beginSQL:        "start transaction with consistent snapshot, read only",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT, querypb.ExecuteOptions_READ_ONLY},
	}, {
		beginSQL:        "start transaction with consistent snapshot, read write",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_CONSISTENT_SNAPSHOT, querypb.ExecuteOptions_READ_WRITE},
	}, {
		beginSQL:        "start transaction read only, with consistent snapshot",
		expTxAccessMode: []querypb.ExecuteOptions_TransactionAccessMode{querypb.ExecuteOptions_READ_ONLY, querypb.ExecuteOptions_CONSISTENT_SNAPSHOT},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.beginSQL, func(t *testing.T) {
			_, err := executor.Execute(ctx, "TestExecutorStartTxnStmt", session, tcase.beginSQL, nil)
			require.NoError(t, err)

			assert.Equal(t, tcase.expTxAccessMode, session.GetOrCreateOptions().TransactionAccessMode)

			_, err = executor.Execute(ctx, "TestExecutorStartTxnStmt", session, "rollback", nil)
			require.NoError(t, err)

		})
	}
}

func exec(executor *Executor, session *SafeSession, sql string) (*sqltypes.Result, error) {
	return executor.Execute(context.Background(), "TestExecute", session, sql, nil)
}

func makeComments(text string) sqlparser.MarginComments {
	return sqlparser.MarginComments{Trailing: text}
}
