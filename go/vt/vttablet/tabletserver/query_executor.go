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

package tabletserver

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vttablet/jobcontroller"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	p "vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// QueryExecutor is used for executing a query request.
type QueryExecutor struct {
	query             string
	marginComments    sqlparser.MarginComments
	bindVars          map[string]*querypb.BindVariable
	connID            int64
	options           *querypb.ExecuteOptions
	plan              *TabletPlan
	ctx               context.Context
	logStats          *tabletenv.LogStats
	tsv               *TabletServer
	tabletType        topodatapb.TabletType
	setting           *pools.Setting
	matchedActionList []ActionInterface
	calledActionList  []ActionInterface
}

const (
	streamRowsSize         = 256
	maxQueryBufferDuration = 10 * time.Second
)

var (
	streamResultPool = sync.Pool{New: func() any {
		return &sqltypes.Result{
			Rows: make([][]sqltypes.Value, 0, streamRowsSize),
		}
	}}
	sequenceFields = []*querypb.Field{
		{
			Name: "nextval",
			Type: sqltypes.Int64,
		},
	}
)

func returnStreamResult(result *sqltypes.Result) error {
	// only return large results slices to the pool
	if cap(result.Rows) >= streamRowsSize {
		rows := result.Rows[:0]
		*result = sqltypes.Result{}
		result.Rows = rows
		streamResultPool.Put(result)
	}
	return nil
}

func allocStreamResult() *sqltypes.Result {
	return streamResultPool.Get().(*sqltypes.Result)
}

func (qre *QueryExecutor) shouldConsolidate() bool {
	co := qre.options.GetConsolidator()
	switch co {
	case querypb.ExecuteOptions_CONSOLIDATOR_DISABLED:
		return false
	case querypb.ExecuteOptions_CONSOLIDATOR_ENABLED:
		return true
	case querypb.ExecuteOptions_CONSOLIDATOR_ENABLED_REPLICAS:
		return qre.tabletType != topodatapb.TabletType_PRIMARY
	default:
		cm := qre.tsv.qe.consolidatorMode.Get()
		return cm == tabletenv.Enable || (cm == tabletenv.NotOnPrimary && qre.tabletType != topodatapb.TabletType_PRIMARY)
	}
}

// Execute performs a non-streaming query execution.
func (qre *QueryExecutor) Execute() (reply *sqltypes.Result, err error) {
	planName := qre.plan.PlanID.String()
	qre.logStats.PlanType = planName
	defer func(start time.Time) {
		duration := time.Since(start)
		qre.tsv.stats.QueryTimings.Add(planName, duration)
		qre.recordUserQuery("Execute", int64(duration))

		mysqlTime := qre.logStats.MysqlResponseTime
		tableName := qre.plan.TableName()
		if tableName == "" {
			tableName = "Join"
		}

		if reply == nil {
			qre.tsv.qe.AddStats(qre.plan.PlanID, tableName, 1, duration, mysqlTime, 0, 0, 1)
			qre.plan.AddStats(1, duration, mysqlTime, 0, 0, 1)
			return
		}
		qre.tsv.qe.AddStats(qre.plan.PlanID, tableName, 1, duration, mysqlTime, int64(reply.RowsAffected), int64(len(reply.Rows)), 0)
		qre.plan.AddStats(1, duration, mysqlTime, reply.RowsAffected, uint64(len(reply.Rows)), 0)
		qre.logStats.RowsAffected = int(reply.RowsAffected)
		qre.logStats.Rows = reply.Rows
		qre.tsv.Stats().ResultHistogram.Add(int64(len(reply.Rows)))
	}(time.Now())

	qre.initDatabaseProxyFilter()
	if isInspectFilter(qre.marginComments.Leading) {
		return qre.getFilterInfo()
	}

	qr, err := qre.runActionListBeforeExecution()

	defer func() {
		reply, err = qre.runActionListAfterExecution(reply, err)
	}()

	if qr != nil || err != nil {
		return qr, err
	}

	if err = qre.checkPermissions(); err != nil {
		return nil, err
	}

	if qre.plan.PlanID == p.PlanNextval {
		return qre.execNextval()
	}

	if qre.connID != 0 {
		var conn *StatefulConnection
		// Need upfront connection for DMLs and transactions
		conn, err = qre.tsv.te.txPool.GetAndLock(qre.connID, "for query")
		if err != nil {
			return nil, err
		}
		defer conn.Unlock()
		if qre.setting != nil {
			if err = conn.ApplySetting(qre.ctx, qre.setting); err != nil {
				return nil, vterrors.Wrap(err, "failed to execute system setting on the connection")
			}
		}
		return qre.txConnExec(conn)
	}

	switch qre.plan.PlanID {
	case p.PlanSelect, p.PlanSelectImpossible, p.PlanShow:
		maxrows := qre.getSelectLimit()
		qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
		if qre.bindVars[sqltypes.BvReplaceSchemaName] != nil {
			qre.bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(qre.tsv.config.DB.DBName)
		}
		qr, err := qre.execSelect()
		if err != nil {
			return nil, err
		}
		if err := qre.verifyRowCount(int64(len(qr.Rows)), maxrows); err != nil {
			return nil, err
		}
		return qr, nil
	case p.PlanOtherRead, p.PlanOtherAdmin, p.PlanFlush, p.PlanSavepoint, p.PlanRelease, p.PlanSRollback:
		return qre.execOther()
	case p.PlanInsert, p.PlanUpdate, p.PlanDelete, p.PlanInsertMessage, p.PlanDDL, p.PlanLoad:
		return qre.execAutocommit(qre.txConnExec)
	case p.PlanViewDDL:
		switch qre.plan.FullStmt.(type) {
		case *sqlparser.DropView:
			return qre.execAutocommit(qre.execDropViewDDL)
		default:
			return qre.execAsTransaction(qre.execViewDDL)
		}
	case p.PlanUpdateLimit, p.PlanDeleteLimit:
		return qre.execAsTransaction(qre.txConnExec)
	case p.PlanCallProc:
		return qre.execCallProc()
	case p.PlanAlterMigration:
		return qre.execAlterMigration()
	case p.PlanAlterDMLJob:
		return qre.execAlterDMLJob()
	case p.PlanRevertMigration:
		return qre.execRevertMigration()
	case p.PlanShowMigrationLogs:
		return qre.execShowMigrationLogs()
	case p.PlanShowThrottledApps:
		return qre.execShowThrottledApps()
	case p.PlanShowThrottlerStatus:
		return qre.execShowThrottlerStatus()
	case p.PlanSet:
		if qre.setting == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "[BUG] %s not allowed without setting connection", qre.query)
		}
		// The execution is not required as this setting will be applied when any other query type is executed.
		return &sqltypes.Result{}, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] %s unexpected plan type", qre.plan.PlanID.String())
}

func (qre *QueryExecutor) txExecViewDDL(conn *StatefulConnection) (*sqltypes.Result, error) {
	// we should commit dml in transactions before executing view DDLs
	_, err := conn.Exec(qre.ctx, "commit", 1000, false)
	if err != nil {
		return nil, err
	}

	switch qre.plan.FullStmt.(type) {
	case *sqlparser.DropView:
		return qre.execAutocommit(qre.execDropViewDDL)
	default:
		return qre.execAsTransaction(qre.execViewDDL)
	}
}

func (qre *QueryExecutor) execAutocommit(f func(conn *StatefulConnection) (*sqltypes.Result, error)) (reply *sqltypes.Result, err error) {
	if qre.options == nil {
		qre.options = &querypb.ExecuteOptions{}
	} else {
		qre.options = proto.Clone(qre.options).(*querypb.ExecuteOptions)
	}
	qre.options.TransactionIsolation = querypb.ExecuteOptions_AUTOCOMMIT

	conn, _, _, err := qre.tsv.te.txPool.Begin(qre.ctx, qre.options, false, 0, nil, qre.setting)

	if err != nil {
		return nil, err
	}
	defer qre.tsv.te.txPool.RollbackAndRelease(qre.ctx, conn)

	return f(conn)
}

func (qre *QueryExecutor) execAsTransaction(f func(conn *StatefulConnection) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	conn, beginSQL, _, err := qre.tsv.te.txPool.Begin(qre.ctx, qre.options, false, 0, nil, qre.setting)
	if err != nil {
		return nil, err
	}
	defer qre.tsv.te.txPool.RollbackAndRelease(qre.ctx, conn)
	qre.logStats.AddRewrittenSQL(beginSQL, time.Now())

	result, err := f(conn)
	if err != nil {
		// dbConn is nil, it means the transaction was aborted.
		// If so, we should not relog the rollback.
		// TODO(sougou): these txPool functions should take the logstats
		// and log any statements they issue. This needs to be done as
		// a separate refactor because it impacts lot of code.
		if conn.IsInTransaction() {
			defer qre.logStats.AddRewrittenSQL("rollback", time.Now())
		}
		return nil, err
	}

	defer qre.logStats.AddRewrittenSQL("commit", time.Now())
	if _, _, err := qre.tsv.te.txPool.Commit(qre.ctx, conn); err != nil {
		return nil, err
	}
	return result, nil
}

func (qre *QueryExecutor) txConnExec(conn *StatefulConnection) (*sqltypes.Result, error) {
	switch qre.plan.PlanID {
	case p.PlanInsert, p.PlanUpdate, p.PlanDelete, p.PlanSet:
		return qre.txFetch(conn, true)
	case p.PlanInsertMessage:
		qre.bindVars["#time_now"] = sqltypes.Int64BindVariable(time.Now().UnixNano())
		return qre.txFetch(conn, true)
	case p.PlanUpdateLimit, p.PlanDeleteLimit:
		return qre.execDMLLimit(conn)
	case p.PlanOtherRead, p.PlanOtherAdmin, p.PlanFlush:
		return qre.execStatefulConn(conn, qre.query, true)
	case p.PlanSavepoint, p.PlanRelease, p.PlanSRollback:
		return qre.execStatefulConn(conn, qre.query, true)
	case p.PlanSelect, p.PlanSelectImpossible, p.PlanShow, p.PlanSelectLockFunc:
		maxrows := qre.getSelectLimit()
		qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
		if qre.bindVars[sqltypes.BvReplaceSchemaName] != nil {
			qre.bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(qre.tsv.config.DB.DBName)
		}
		qr, err := qre.txFetch(conn, false)
		if err != nil {
			return nil, err
		}
		if err := qre.verifyRowCount(int64(len(qr.Rows)), maxrows); err != nil {
			return nil, err
		}
		return qr, nil
	case p.PlanDDL:
		return qre.execDDL(conn)
	case p.PlanLoad:
		return qre.execLoad(conn)
	case p.PlanCallProc:
		return qre.execProc(conn)
	case p.PlanViewDDL:
		return qre.txExecViewDDL(conn)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] %s unexpected plan type", qre.plan.PlanID.String())
}

func (qre *QueryExecutor) execViewDDL(conn *StatefulConnection) (*sqltypes.Result, error) {
	var err error
	switch stmt := qre.plan.FullStmt.(type) {
	case *sqlparser.CreateView:
		_, err = qre.execCreateViewDDL(conn, stmt)
	case *sqlparser.AlterView:
		_, err = qre.execAlterViewDDL(conn, stmt)
	default:
		err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected view DDL type: %T", qre.plan.FullStmt)
	}
	if err != nil {
		return nil, err
	}
	// We need to use a different connection for executing the DDL on MySQL
	// because the previous DMLs are running in a transaction and we don't want to autocommit
	// those changes.
	ddlConn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer ddlConn.Recycle()
	// If MySQL fails, then we will Rollback the changes.
	return ddlConn.Exec(qre.ctx, sqlparser.String(qre.plan.FullStmt), 1000, true)
}

func (qre *QueryExecutor) getQueryCreateViewFromViews(stmt *sqlparser.CreateView) (string, error) {
	bindVars := generateBindVarsForViewDDLInsert(stmt)
	if _, exist := bindVars["table_schema"]; !exist {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table schema is not specified")
	}
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, bindVars)
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (qre *QueryExecutor) execCreateViewDDL(conn *StatefulConnection, stmt *sqlparser.CreateView) (*sqltypes.Result, error) {
	sql, err := qre.getQueryCreateViewFromViews(stmt)
	if err != nil {
		return nil, err
	}

	qr, err := execWithDDLView(qre.ctx, conn, sql)
	if err != nil {
		sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
		// If it is a MySQL error and its code is of duplicate entry,
		// then we would return duplicate create view error.
		if isSQLErr && sqlErr.Number() == mysql.ERDupEntry {
			return nil, vterrors.Errorf(vtrpcpb.Code_ALREADY_EXISTS, "Table '%s' already exists", stmt.ViewName.Name.String())
		}
		return nil, err
	}
	return qr, nil
}

func (qre *QueryExecutor) getQueryAlterViewFromViews(stmt *sqlparser.AlterView) (string, error) {
	createViewDDL := &sqlparser.CreateView{
		ViewName:    stmt.ViewName,
		Algorithm:   stmt.Algorithm,
		Definer:     stmt.Definer,
		Security:    stmt.Security,
		Columns:     stmt.Columns,
		Select:      stmt.Select,
		CheckOption: stmt.CheckOption,
		Comments:    stmt.Comments,
	}
	bindVars := generateBindVarsForViewDDLInsert(createViewDDL)
	if _, exist := bindVars["table_schema"]; !exist {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table schema is not specified")
	}
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, bindVars)
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (qre *QueryExecutor) execAlterViewDDL(conn *StatefulConnection, stmt *sqlparser.AlterView) (*sqltypes.Result, error) {
	sql, err := qre.getQueryAlterViewFromViews(stmt)
	if err != nil {
		return nil, err
	}
	qr, err := execWithDDLView(qre.ctx, conn, sql)
	if err != nil {
		return nil, err
	}
	if qr.RowsAffected == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "Table '%s' does not exist", stmt.ViewName.Name.String())
	}
	return qr, nil
}

func getQueryDropViewFromViews(stmt *sqlparser.DropView) string {
	pairsOfTableSchemaAndName := " where "
	first := true
	for _, view := range stmt.FromTables {
		subWhereClaus := ""
		if !first {
			subWhereClaus = " or "
		}
		subWhereClaus += fmt.Sprintf("(table_schema = '%s' and table_name = '%s')", view.Qualifier.String(), view.Name.String())
		first = false
		pairsOfTableSchemaAndName += subWhereClaus
	}

	finalQuery := mysql.DeleteFromViewsTableWithoutCondition + pairsOfTableSchemaAndName
	return finalQuery
}

func (qre *QueryExecutor) execDropViewDDL(conn *StatefulConnection) (*sqltypes.Result, error) {
	stmt := qre.plan.FullStmt.(*sqlparser.DropView)

	finalQuery := getQueryDropViewFromViews(stmt)
	_, err := execWithDDLView(qre.ctx, conn, finalQuery)
	if err != nil {
		return nil, err
	}

	// Drop the view on MySQL too.
	return conn.Exec(qre.ctx, sqlparser.String(qre.plan.FullStmt), 1000, true)
}

func execWithDDLView(ctx context.Context, conn *StatefulConnection, sql string) (*sqltypes.Result, error) {
	return conn.Exec(ctx, sql, 10000, true)
}

// Stream performs a streaming query execution.
func (qre *QueryExecutor) Stream(callback StreamCallback) error {
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		qre.tsv.stats.QueryTimings.Record(qre.plan.PlanID.String(), start)
		qre.recordUserQuery("Stream", int64(time.Since(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	switch qre.plan.PlanID {
	case p.PlanSelectStream:
		if qre.bindVars[sqltypes.BvReplaceSchemaName] != nil {
			qre.bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(qre.tsv.config.DB.DBName)
		}
	}

	sql, sqlWithoutComments, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return err
	}

	var replaceKeyspace string
	if sqltypes.IncludeFieldsOrDefault(qre.options) == querypb.ExecuteOptions_ALL && qre.tsv.sm.target.Keyspace != qre.tsv.config.DB.DBName {
		replaceKeyspace = qre.tsv.sm.target.Keyspace
	}

	if consolidator := qre.tsv.qe.streamConsolidator; consolidator != nil {
		if qre.connID == 0 && qre.plan.PlanID == p.PlanSelectStream && qre.shouldConsolidate() {
			return consolidator.Consolidate(qre.logStats, sqlWithoutComments, callback,
				func(callback StreamCallback) error {
					dbConn, err := qre.getStreamConn()
					if err != nil {
						return err
					}
					defer dbConn.Recycle()
					_, waitGtidErr := qre.execWaitForExecutedGtidSetIfNecessary(dbConn)
					if waitGtidErr != nil {
						return waitGtidErr
					}
					return qre.execStreamSQL(dbConn, qre.connID != 0, sql, func(result *sqltypes.Result) error {
						// this stream result is potentially used by more than one client, so
						// the consolidator will return it to the pool once it knows it's no longer
						// being shared

						if replaceKeyspace != "" {
							result.ReplaceKeyspace(replaceKeyspace)
						}
						return callback(result)
					})
				})
		}
	}

	// if we have a transaction id, let's use the txPool for this query
	var conn *connpool.DBConn
	if qre.connID != 0 {
		txConn, err := qre.tsv.te.txPool.GetAndLock(qre.connID, "for streaming query")
		if err != nil {
			return err
		}
		defer txConn.Unlock()
		if qre.setting != nil {
			if err = txConn.ApplySetting(qre.ctx, qre.setting); err != nil {
				return vterrors.Wrap(err, "failed to execute system setting on the connection")
			}
		}
		conn = txConn.UnderlyingDBConn()
	} else {
		dbConn, err := qre.getStreamConn()
		if err != nil {
			return err
		}
		defer dbConn.Recycle()
		conn = dbConn
	}

	_, waitGtidErr := qre.execWaitForExecutedGtidSetIfNecessary(conn)
	if waitGtidErr != nil {
		return waitGtidErr
	}
	return qre.execStreamSQL(conn, qre.connID != 0, sql, func(result *sqltypes.Result) error {
		// this stream result is only used by the calling client, so it can be
		// returned to the pool once the callback has fully returned
		defer returnStreamResult(result)

		if replaceKeyspace != "" {
			result.ReplaceKeyspace(replaceKeyspace)
		}
		return callback(result)
	})
}

// MessageStream streams messages from a message table.
func (qre *QueryExecutor) MessageStream(callback StreamCallback) error {
	qre.logStats.OriginalSQL = qre.query
	qre.logStats.PlanType = qre.plan.PlanID.String()

	defer func(start time.Time) {
		qre.tsv.stats.QueryTimings.Record(qre.plan.PlanID.String(), start)
		qre.recordUserQuery("MessageStream", int64(time.Since(start)))
	}(time.Now())

	if err := qre.checkPermissions(); err != nil {
		return err
	}

	done, err := qre.tsv.messager.Subscribe(qre.ctx, qre.plan.TableName(), func(r *sqltypes.Result) error {
		select {
		case <-qre.ctx.Done():
			return io.EOF
		default:
		}
		return callback(r)
	})
	if err != nil {
		return err
	}
	<-done
	return nil
}

func (qre *QueryExecutor) initDatabaseProxyFilter() {
	remoteAddr := ""
	username := ""
	ci, ok := callinfo.FromContext(qre.ctx)
	if ok {
		remoteAddr = ci.RemoteAddr()
		username = ci.Username()
	}

	pluginList := GetActionList(qre.plan.Rules, remoteAddr, username, qre.bindVars, qre.marginComments)
	qre.matchedActionList = pluginList
}

// runActionListBeforeExecution runs the action list and returns the first error it encounters.
func (qre *QueryExecutor) runActionListBeforeExecution() (*sqltypes.Result, error) {
	if len(qre.matchedActionList) == 0 {
		return nil, nil
	}
	for _, a := range qre.matchedActionList {
		startTime := time.Now()
		// execute the filter action
		resp := a.BeforeExecution(qre)
		qre.tsv.qe.actionStats.FilterBeforeExecutionTiming.Add(a.GetRule().Name, time.Since(startTime))
		qre.calledActionList = append(qre.calledActionList, a)
		if resp.Err != nil {
			qre.tsv.qe.actionStats.FilterErrorCounts.Add(a.GetRule().Name, 1)
		}
		if resp.Reply != nil || resp.Err != nil {
			return resp.Reply, resp.Err
		}
	}
	return nil, nil
}

// runActionListAfterExecution runs the action list and returns the first error it encounters in reverse order.
func (qre *QueryExecutor) runActionListAfterExecution(reply *sqltypes.Result, err error) (*sqltypes.Result, error) {
	newReply, newErr := reply, err
	if len(qre.calledActionList) == 0 {
		return newReply, newErr
	}

	for i := len(qre.calledActionList) - 1; i >= 0; i-- {
		a := qre.calledActionList[i]
		startTime := time.Now()
		// execute the filter action
		resp := a.AfterExecution(qre, newReply, newErr)
		qre.tsv.qe.actionStats.FilterAfterExecutionTiming.Add(a.GetRule().Name, time.Since(startTime))
		if resp.Err != nil {
			qre.tsv.qe.actionStats.FilterErrorCounts.Add(a.GetRule().Name, 1)
		}
		newReply, newErr = resp.Reply, resp.Err
	}
	return newReply, newErr
}

// checkPermissions returns an error if the query does not pass all checks
// (denied query, table ACL).
func (qre *QueryExecutor) checkPermissions() error {
	// Skip permissions check if the context is local.
	if tabletenv.IsLocalContext(qre.ctx) || (qre.options != nil && !qre.options.AccountVerificationEnabled) {
		return nil
	}

	// Check if the query relates to a table that is in the denylist.
	username := ""
	ci, ok := callinfo.FromContext(qre.ctx)
	if ok {
		username = ci.Username()
	}

	// Skip ACL check for queries against the dummy dual table
	if qre.plan.TableName() == "dual" {
		return nil
	}

	// Skip the ACL check if the connecting user is an exempted superuser.
	if qre.tsv.qe.exemptACL != nil && qre.tsv.qe.exemptACL.IsMember(&querypb.VTGateCallerID{Username: username}) {
		qre.tsv.qe.tableaclExemptCount.Add(1)
		return nil
	}

	callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
	if callerID == nil {
		if qre.tsv.qe.strictTableACL {
			return vterrors.Errorf(vtrpcpb.Code_UNAUTHENTICATED, "missing caller id")
		}
		return nil
	}

	// Skip the ACL check if the caller id is an exempted superuser.
	if qre.tsv.qe.exemptACL != nil && qre.tsv.qe.exemptACL.IsMember(callerID) {
		qre.tsv.qe.tableaclExemptCount.Add(1)
		return nil
	}

	for i, auth := range qre.plan.Authorized {
		if err := qre.checkAccess(auth, qre.plan.Permissions[i].TableName, callerID); err != nil {
			return err
		}
	}

	return nil
}

func (qre *QueryExecutor) checkAccess(authorized []*tableacl.ACLResult, tableName string, callerID *querypb.VTGateCallerID) error {
	statsKey := []string{tableName, "EmptyGroupName", qre.plan.PlanID.String(), callerID.Username}
	isPass := false
	for _, acl := range authorized {
		if acl.IsMember(callerID) {
			isPass = true
			statsKey[1] = acl.GroupName
		}
	}
	if !isPass {
		if qre.tsv.qe.enableTableACLDryRun {
			qre.tsv.Stats().TableaclPseudoDenied.Add(statsKey, 1)
			return nil
		}

		// Skip ACL check for queries against the dummy dual table
		if tableName == "dual" {
			return nil
		}

		if qre.tsv.qe.strictTableACL {
			groupStr := ""
			if len(callerID.Groups) > 0 {
				groupStr = fmt.Sprintf(", in groups [%s],", strings.Join(callerID.Groups, ", "))
			}
			errStr := fmt.Sprintf("%s command denied to user '%s'%s for table '%s' (ACL check error)", qre.plan.PlanID.String(), callerID.Username, groupStr, tableName)
			qre.tsv.Stats().TableaclDenied.Add(statsKey, 1)
			qre.tsv.qe.accessCheckerLogger.Infof("%s", errStr)
			return vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "%s", errStr)
		}
		return nil
	}
	qre.tsv.Stats().TableaclAllowed.Add(statsKey, 1)
	return nil
}

func (qre *QueryExecutor) execDDL(conn *StatefulConnection) (*sqltypes.Result, error) {
	// Let's see if this is a normal DDL statement or an Online DDL statement.
	// An Online DDL statement is identified by /*vt+ .. */ comment with expected directives, like uuid etc.
	if onlineDDL, err := schema.OnlineDDLFromCommentedStatement(qre.plan.FullStmt); err == nil {
		// Parsing is successful.
		if !onlineDDL.Strategy.IsDirect() {
			// This is an online DDL.
			return qre.tsv.onlineDDLExecutor.SubmitMigration(qre.ctx, qre.plan.FullStmt)
		}
	}

	isTemporaryTable := false
	if ddlStmt, ok := qre.plan.FullStmt.(sqlparser.DDLStatement); ok {
		isTemporaryTable = ddlStmt.IsTemporary()
	}
	if !isTemporaryTable {
		// Temporary tables are limited to the session creating them. There is no need to Reload()
		// the table because other connections will not be able to see the table anyway.
		defer func() {
			// Call se.Reload() with includeStats=false as obtaining table
			// size stats involves joining `information_schema.tables`,
			// which can be very costly on systems with a large number of
			// tables.
			//
			// Instead of synchronously recalculating table size stats
			// after every DDL, let them be outdated until the periodic
			// schema reload fixes it.
			if err := qre.tsv.se.ReloadAtEx(qre.ctx, mysql.Position{}, true); err != nil {
				log.Errorf("failed to reload schema %v", err)
			}
		}()
	}
	sql := qre.query
	// If FullQuery is not nil, then the DDL query was fully parsed
	// and we should use the ast to generate the query instead.
	if qre.plan.FullQuery != nil {
		var err error
		sql, _, err = qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
		if err != nil {
			return nil, err
		}
	}
	result, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, err
	}
	// Only perform this operation when the connection has transaction open.
	// TODO: This actually does not retain the old transaction. We should see how to provide correct behaviour to client.
	if conn.txProps != nil {
		err = qre.BeginAgain(qre.ctx, conn)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (qre *QueryExecutor) execLoad(conn *StatefulConnection) (*sqltypes.Result, error) {
	result, err := qre.execStatefulConn(conn, qre.query, true)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// BeginAgain commits the existing transaction and begins a new one
func (*QueryExecutor) BeginAgain(ctx context.Context, dc *StatefulConnection) error {
	if dc.IsClosed() || dc.TxProperties().Autocommit {
		return nil
	}
	if _, err := dc.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := dc.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

func (qre *QueryExecutor) execNextval() (*sqltypes.Result, error) {
	env := evalengine.EnvWithBindVars(qre.bindVars, collations.Unknown)
	result, err := env.Evaluate(qre.plan.NextCount)
	if err != nil {
		return nil, err
	}
	tableName := qre.plan.TableName()
	v := result.Value()
	inc, err := v.ToInt64()
	if err != nil || inc < 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid increment for sequence %s: %s", tableName, v.String())
	}

	t := qre.plan.Table
	t.SequenceInfo.Lock()
	defer t.SequenceInfo.Unlock()
	if t.SequenceInfo.NextVal == 0 || t.SequenceInfo.NextVal+inc > t.SequenceInfo.LastVal {
		_, err := qre.execAsTransaction(func(conn *StatefulConnection) (*sqltypes.Result, error) {
			query := fmt.Sprintf("select next_id, cache from %s where id = 0 for update", tableName)
			qr, err := qre.execStatefulConn(conn, query, false)
			if err != nil {
				return nil, err
			}
			if len(qr.Rows) != 1 {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected rows from reading sequence %s (possible mis-route): %d", tableName, len(qr.Rows))
			}
			nextID, err := evalengine.ToInt64(qr.Rows[0][0])
			if err != nil {
				return nil, vterrors.Wrapf(err, "error loading sequence %s", tableName)
			}
			// If LastVal does not match next ID, then either:
			// VTTablet just started, and we're initializing the cache, or
			// Someone reset the id underneath us.
			if t.SequenceInfo.LastVal != nextID {
				if nextID < t.SequenceInfo.LastVal {
					log.Warningf("Sequence next ID value %v is below the currently cached max %v, updating it to max", nextID, t.SequenceInfo.LastVal)
					nextID = t.SequenceInfo.LastVal
				}
				t.SequenceInfo.NextVal = nextID
				t.SequenceInfo.LastVal = nextID
			}
			cache, err := evalengine.ToInt64(qr.Rows[0][1])
			if err != nil {
				return nil, vterrors.Wrapf(err, "error loading sequence %s", tableName)
			}
			if cache < 1 {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid cache value for sequence %s: %d", tableName, cache)
			}
			newLast := nextID + cache
			for newLast < t.SequenceInfo.NextVal+inc {
				newLast += cache
			}
			query = fmt.Sprintf("update %s set next_id = %d where id = 0", tableName, newLast)
			conn.TxProperties().RecordQuery(query)
			_, err = qre.execStatefulConn(conn, query, false)
			if err != nil {
				return nil, err
			}
			t.SequenceInfo.LastVal = newLast
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
	}
	ret := t.SequenceInfo.NextVal
	t.SequenceInfo.NextVal += inc
	return &sqltypes.Result{
		Fields: sequenceFields,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(ret),
		}},
	}, nil
}

// execSelect sends a query to mysql only if another identical query is not running. Otherwise, it waits and
// reuses the result. If the plan is missing field info, it sends the query to mysql requesting full info.
func (qre *QueryExecutor) execSelect() (*sqltypes.Result, error) {
	sql, sqlWithoutComments, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	wantFields := true
	// If the query is a read after write, we need to add the wait gtid prefix
	sql, waitGtidPrefixAdded := qre.addPrefixWaitGtid(sql)
	// Check tablet type.
	if qre.shouldConsolidate() {
		q, original := qre.tsv.qe.consolidator.Create(sqlWithoutComments)
		if original {
			defer q.Broadcast()
			conn, err := qre.getConn()

			if err != nil {
				q.Err = err
			} else {
				defer conn.Recycle()
				q.Result, q.Err = qre.execDBConn(conn, sql, wantFields)
				if waitGtidPrefixAdded {
					q.Result, q.Err = qre.discardWaitGtidResponse(q.Result.(*sqltypes.Result), q.Err, conn, wantFields)
				}
			}
		} else {
			qre.logStats.QuerySources |= tabletenv.QuerySourceConsolidator
			startTime := time.Now()
			q.Wait()
			qre.tsv.stats.WaitTimings.Record("Consolidations", startTime)
		}
		if q.Err != nil {
			return nil, q.Err
		}
		return q.Result.(*sqltypes.Result), nil
	}
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	res, err := qre.execDBConn(conn, sql, wantFields)
	if waitGtidPrefixAdded {
		res, err = qre.discardWaitGtidResponse(res, err, conn, wantFields)
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// addPrefixWaitGtid adds a prefix to the query to wait for the gtid to be replicated.
// make sure to call discardWaitGtidResponse if waitGtidPrefixAdded returns true.
func (qre *QueryExecutor) addPrefixWaitGtid(sql string) (newSQL string, waitGtidPrefixAdded bool) {
	if qre.options == nil || qre.options.ReadAfterWriteGtid == "" {
		return sql, false
	}
	var buf strings.Builder
	buf.Grow(len(qre.options.GetReadAfterWriteGtid()) + len(sql) + 64)
	buf.WriteString(fmt.Sprintf("SELECT WAIT_FOR_EXECUTED_GTID_SET('%s', %v);",
		qre.options.GetReadAfterWriteGtid(), qre.options.GetReadAfterWriteTimeout()))
	buf.WriteString(sql)
	newSQL = buf.String()
	return newSQL, true
}

const WaitGtidTimeoutFlag = "1"

// discardWaitGtidResponse discards the wait gtid response and returns the last result.
func (qre *QueryExecutor) discardWaitGtidResponse(res *sqltypes.Result, err error, conn *connpool.DBConn, wantFields bool) (*sqltypes.Result, error) {
	if err != nil {
		return nil, err
	}
	if !res.IsMoreResultsExists() {
		// should not happen
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "wait for gtid response is not complete")
	}
	waitGtidRes := res.Rows[0][0].ToString()
	// we need to fetch all the results and discard them.
	// Otherwise, the connection will be left in a bad state.
	// the last result will be returned to the caller.
	userSQLRes, userSQLErr := conn.FetchNext(qre.ctx, int(qre.tsv.qe.maxResultSize.Get()), wantFields)
	if waitGtidRes == WaitGtidTimeoutFlag {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "wait for gtid timeout")
	}
	return userSQLRes, userSQLErr
}

// execWaitForExecutedGtidSetIfNecessary executes a query to wait for the gtid to be replicated.
// It returns true if the query is executed, false if the query is not executed.
// It returns an error if the query is executed and the GTID is not replicated.
func (qre *QueryExecutor) execWaitForExecutedGtidSetIfNecessary(conn *connpool.DBConn) (executed bool, err error) {
	sql, waitGtidPrefixAdded := qre.addPrefixWaitGtid("")
	if !waitGtidPrefixAdded || sql == "" {
		return false, nil
	}
	res, err := qre.execDBConn(conn, sql, true)
	if err != nil {
		return true, err
	}
	waitGtidRes := res.Rows[0][0].ToString()
	if waitGtidRes == WaitGtidTimeoutFlag {
		return true, vterrors.Errorf(vtrpcpb.Code_ABORTED, "wait for gtid timeout")
	}
	return true, nil
}

func (qre *QueryExecutor) execDMLLimit(conn *StatefulConnection) (*sqltypes.Result, error) {
	maxrows := qre.tsv.qe.maxResultSize.Get()
	qre.bindVars["#maxLimit"] = sqltypes.Int64BindVariable(maxrows + 1)
	result, err := qre.txFetch(conn, true)
	if err != nil {
		return nil, err
	}
	if err := qre.verifyRowCount(int64(result.RowsAffected), maxrows); err != nil {
		defer qre.logStats.AddRewrittenSQL("rollback", time.Now())
		_ = qre.tsv.te.txPool.Rollback(qre.ctx, conn)
		return nil, err
	}
	return result, nil
}

func (qre *QueryExecutor) verifyRowCount(count, maxrows int64) error {
	if count > maxrows {
		callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
		return vterrors.Errorf(vtrpcpb.Code_ABORTED, "caller id: %s: row count exceeded %d", callerID.Username, maxrows)
	}
	warnThreshold := qre.tsv.qe.warnResultSize.Get()
	if warnThreshold > 0 && count > warnThreshold {
		callerID := callerid.ImmediateCallerIDFromContext(qre.ctx)
		qre.tsv.Stats().Warnings.Add("ResultsExceeded", 1)
		log.Warningf("caller id: %s row count %v exceeds warning threshold %v: %q", callerID.Username, count, warnThreshold, queryAsString(qre.plan.FullQuery.Query, qre.bindVars, qre.tsv.Config().SanitizeLogMessages))
	}
	return nil
}

func (qre *QueryExecutor) execOther() (*sqltypes.Result, error) {
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return qre.execDBConn(conn, qre.query, true)
}

func (qre *QueryExecutor) getConn() (*connpool.DBConn, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.getConn")
	defer span.Finish()
	var conn *connpool.DBConn
	var err error
	start := time.Now()

	// If the obtained connection does not need to specify a database, create a connection without dbname directly
	// from the non-connection pool
	if qre.setting != nil && qre.setting.GetWithoutDBName() {
		conn, err = qre.tsv.qe.withoutDBConns.Get(ctx, qre.setting)
	} else {
		conn, err = qre.tsv.qe.conns.Get(ctx, qre.setting)
	}

	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Since(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

func (qre *QueryExecutor) getStreamConn() (*connpool.DBConn, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.getStreamConn")
	defer span.Finish()
	var conn *connpool.DBConn
	var err error

	start := time.Now()
	if qre.setting != nil && qre.setting.GetWithoutDBName() {
		conn, err = qre.tsv.qe.streamWithoutDBConns.Get(ctx, qre.setting)
	} else {
		conn, err = qre.tsv.qe.streamConns.Get(ctx, qre.setting)
	}
	switch err {
	case nil:
		qre.logStats.WaitingForConnection += time.Since(start)
		return conn, nil
	case connpool.ErrConnPoolClosed:
		return nil, err
	}
	return nil, err
}

// txFetch fetches from a TxConnection.
func (qre *QueryExecutor) txFetch(conn *StatefulConnection, record bool) (*sqltypes.Result, error) {
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	waitGtidPrefixAdded := false
	if qre.plan.PlanID == p.PlanSelect {
		sql, waitGtidPrefixAdded = qre.addPrefixWaitGtid(sql)
	}
	qr, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, err
	}
	// Only record successful queries.
	if record {
		conn.TxProperties().RecordQuery(sql)
	}
	if waitGtidPrefixAdded {
		qr, err = qre.discardWaitGtidResponse(qr, err, conn.UnderlyingDBConn(), true)
	}
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (qre *QueryExecutor) generateFinalSQL(parsedQuery *sqlparser.ParsedQuery, bindVars map[string]*querypb.BindVariable) (string, string, error) {
	query, err := parsedQuery.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s", err)
	}
	if qre.tsv.config.AnnotateQueries {
		username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(qre.ctx))
		if username == "" {
			username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(qre.ctx))
		}
		var buf strings.Builder
		tabletTypeStr := qre.tsv.sm.target.TabletType.String()
		buf.Grow(8 + len(username) + len(tabletTypeStr))
		buf.WriteString("/* ")
		buf.WriteString(username)
		buf.WriteString("@")
		buf.WriteString(tabletTypeStr)
		buf.WriteString(" */ ")
		buf.WriteString(qre.marginComments.Leading)
		qre.marginComments.Leading = buf.String()
	}

	if qre.marginComments.Leading == "" && qre.marginComments.Trailing == "" {
		return query, query, nil
	}

	var buf strings.Builder
	buf.Grow(len(qre.marginComments.Leading) + len(query) + len(qre.marginComments.Trailing))
	buf.WriteString(qre.marginComments.Leading)
	buf.WriteString(query)
	buf.WriteString(qre.marginComments.Trailing)
	return buf.String(), query, nil
}

func rewriteOUTParamError(err error) error {
	sqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		return err
	}
	if sqlErr.Num == mysql.ErSPNotVarArg {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "OUT and INOUT parameters are not supported")
	}
	return err
}

func (qre *QueryExecutor) execCallProc() (*sqltypes.Result, error) {
	conn, err := qre.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}

	qr, err := qre.execDBConn(conn, sql, true)
	if err != nil {
		return nil, rewriteOUTParamError(err)
	}
	if !qr.IsMoreResultsExists() {
		if qr.IsInTransaction() {
			conn.Close()
			return nil, vterrors.New(vtrpcpb.Code_CANCELED, "Transaction not concluded inside the stored procedure, leaking transaction from stored procedure is not allowed")
		}
		return qr, nil
	}
	err = qre.drainResultSetOnConn(conn)
	if err != nil {
		return nil, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "Multi-Resultset not supported in stored procedure")
}

func (qre *QueryExecutor) execProc(conn *StatefulConnection) (*sqltypes.Result, error) {
	beforeInTx := conn.IsInTransaction()
	sql, _, err := qre.generateFinalSQL(qre.plan.FullQuery, qre.bindVars)
	if err != nil {
		return nil, err
	}
	qr, err := qre.execStatefulConn(conn, sql, true)
	if err != nil {
		return nil, rewriteOUTParamError(err)
	}
	if !qr.IsMoreResultsExists() {
		afterInTx := qr.IsInTransaction()
		if beforeInTx != afterInTx {
			conn.Close()
			return nil, vterrors.New(vtrpcpb.Code_CANCELED, "Transaction state change inside the stored procedure is not allowed")
		}
		return qr, nil
	}
	err = qre.drainResultSetOnConn(conn.UnderlyingDBConn())
	if err != nil {
		return nil, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "Multi-Resultset not supported in stored procedure")
}

func (qre *QueryExecutor) execAlterMigration() (*sqltypes.Result, error) {
	alterMigration, ok := qre.plan.FullStmt.(*sqlparser.AlterMigration)
	if !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting ALTER VITESS_MIGRATION plan")
	}

	switch alterMigration.Type {
	case sqlparser.RetryMigrationType:
		return qre.tsv.onlineDDLExecutor.RetryMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.CleanupMigrationType:
		return qre.tsv.onlineDDLExecutor.CleanupMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.LaunchMigrationType:
		return qre.tsv.onlineDDLExecutor.LaunchMigration(qre.ctx, alterMigration.UUID, alterMigration.Shards)
	case sqlparser.LaunchAllMigrationType:
		return qre.tsv.onlineDDLExecutor.LaunchMigrations(qre.ctx)
	case sqlparser.CompleteMigrationType:
		return qre.tsv.onlineDDLExecutor.CompleteMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.CompleteAllMigrationType:
		return qre.tsv.onlineDDLExecutor.CompletePendingMigrations(qre.ctx)
	case sqlparser.CancelMigrationType:
		return qre.tsv.onlineDDLExecutor.CancelMigration(qre.ctx, alterMigration.UUID, "CANCEL issued by user", true)
	case sqlparser.CancelAllMigrationType:
		return qre.tsv.onlineDDLExecutor.CancelPendingMigrations(qre.ctx, "CANCEL ALL issued by user", true)
	case sqlparser.ThrottleMigrationType:
		return qre.tsv.onlineDDLExecutor.ThrottleMigration(qre.ctx, alterMigration.UUID, alterMigration.Expire, alterMigration.Ratio)
	case sqlparser.ThrottleAllMigrationType:
		return qre.tsv.onlineDDLExecutor.ThrottleAllMigrations(qre.ctx, alterMigration.Expire, alterMigration.Ratio)
	case sqlparser.UnthrottleMigrationType:
		return qre.tsv.onlineDDLExecutor.UnthrottleMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.UnthrottleAllMigrationType:
		return qre.tsv.onlineDDLExecutor.UnthrottleAllMigrations(qre.ctx)
	case sqlparser.PauseMigrationType:
		return qre.tsv.onlineDDLExecutor.PauseMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.ResumeMigrationType:
		return qre.tsv.onlineDDLExecutor.ResumeMigration(qre.ctx, alterMigration.UUID)
	case sqlparser.PauseAllMigrationType:
		return qre.tsv.onlineDDLExecutor.PauseAllMigrations(qre.ctx)
	case sqlparser.ResumeAllMigrationType:
		return qre.tsv.onlineDDLExecutor.ResumeAllMigrations(qre.ctx)
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "ALTER VITESS_MIGRATION not implemented")
}

func (qre *QueryExecutor) execAlterDMLJob() (*sqltypes.Result, error) {
	alterDMLJob, ok := qre.plan.FullStmt.(*sqlparser.AlterDMLJob)
	if !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting ALTER DML_JOB plan")
	}
	uuid := alterDMLJob.UUID
	switch alterDMLJob.Type {
	case sqlparser.PauseDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.PauseJob, "", uuid, "", "", "", "", "", "", 0, 0, false, "", false)
	case sqlparser.ResumeDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.ResumeJob, "", uuid, "", "", "", "", "", "", 0, 0, false, "", false)
	case sqlparser.LaunchDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.LaunchJob, "", uuid, "", "", "", "", "", "", 0, 0, false, "", false)
	case sqlparser.CancelDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.CancelJob, "", uuid, "", "", "", "", "", "", 0, 0, false, "", false)
	case sqlparser.ThrottleDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.ThrottleJob, "", uuid, "", "", "", "", alterDMLJob.Expire, alterDMLJob.Ratio.Val, 0, 0, false, "", false)
	case sqlparser.UnthrottleDMLJobType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.UnthrottleJob, "", uuid, "", "", "", "", "", "", 0, 0, false, "", false)
	case sqlparser.SetRunningTimePeriodType:
		return qre.tsv.dmlJonController.HandleRequest(jobcontroller.SetRunningTimePeriod, "", uuid, "", alterDMLJob.TimePeriodStart, alterDMLJob.TimePeriodEnd, alterDMLJob.TimePeriodTimeZone, "", "", 0, 0, false, "", false)
	}
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "ALTER DML_JOB not implemented")
}

func (qre *QueryExecutor) execRevertMigration() (*sqltypes.Result, error) {
	if _, ok := qre.plan.FullStmt.(*sqlparser.RevertMigration); !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting REVERT VITESS_MIGRATION plan")
	}
	return qre.tsv.onlineDDLExecutor.SubmitMigration(qre.ctx, qre.plan.FullStmt)
}

func (qre *QueryExecutor) execShowMigrationLogs() (*sqltypes.Result, error) {
	if showMigrationLogsStmt, ok := qre.plan.FullStmt.(*sqlparser.ShowMigrationLogs); ok {
		return qre.tsv.onlineDDLExecutor.ShowMigrationLogs(qre.ctx, showMigrationLogsStmt)
	}
	return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting SHOW VITESS_MIGRATION plan")
}

func (qre *QueryExecutor) execShowThrottledApps() (*sqltypes.Result, error) {
	if err := qre.tsv.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	if _, ok := qre.plan.FullStmt.(*sqlparser.ShowThrottledApps); !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting SHOW VITESS_THROTTLED_APPS plan")
	}
	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "app",
				Type: sqltypes.VarChar,
			},
			{
				Name: "expire_at",
				Type: sqltypes.Timestamp,
			},
			{
				Name: "ratio",
				Type: sqltypes.Decimal,
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	for _, t := range qre.tsv.lagThrottler.ThrottledApps() {
		result.Rows = append(result.Rows,
			[]sqltypes.Value{
				sqltypes.NewVarChar(t.AppName),
				sqltypes.NewTimestamp(t.ExpireAt.Format(sqltypes.TimestampFormat)),
				sqltypes.NewDecimal(fmt.Sprintf("%v", t.Ratio)),
			})
	}
	return result, nil
}

func (qre *QueryExecutor) execShowThrottlerStatus() (*sqltypes.Result, error) {
	if _, ok := qre.plan.FullStmt.(*sqlparser.ShowThrottlerStatus); !ok {
		return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "Expecting SHOW VITESS_THROTTLER STATUS plan")
	}
	var enabled int32
	if err := qre.tsv.lagThrottler.CheckIsReady(); err == nil {
		enabled = 1
	}
	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "shard",
				Type: sqltypes.VarChar,
			},
			{
				Name: "enabled",
				Type: sqltypes.Int32,
			},
			{
				Name: "threshold",
				Type: sqltypes.Float64,
			},
			{
				Name: "query",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarChar(qre.tsv.sm.target.Shard),
				sqltypes.NewInt32(enabled),
				sqltypes.NewFloat64(qre.tsv.lagThrottler.MetricsThreshold.Get()),
				sqltypes.NewVarChar(qre.tsv.lagThrottler.GetMetricsQuery()),
			},
		},
	}
	return result, nil
}

func (qre *QueryExecutor) drainResultSetOnConn(conn *connpool.DBConn) error {
	more := true
	for more {
		qr, err := conn.FetchNext(qre.ctx, int(qre.getSelectLimit()), true)
		if err != nil {
			return err
		}
		more = qr.IsMoreResultsExists()
	}
	return nil
}

func (qre *QueryExecutor) getSelectLimit() int64 {
	return qre.tsv.qe.maxResultSize.Get()
}

func (qre *QueryExecutor) execDBConn(conn *connpool.DBConn, sql string, wantfields bool) (*sqltypes.Result, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execDBConn")
	defer span.Finish()

	defer qre.logStats.AddRewrittenSQL(sql, time.Now())

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.statelessql.Add(qd)
	defer qre.tsv.statelessql.Remove(qd)

	return conn.Exec(ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStatefulConn(conn *StatefulConnection, sql string, wantfields bool) (*sqltypes.Result, error) {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execStatefulConn")
	defer span.Finish()

	defer qre.logStats.AddRewrittenSQL(sql, time.Now())

	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	qre.tsv.statefulql.Add(qd)
	defer qre.tsv.statefulql.Remove(qd)

	return conn.Exec(ctx, sql, int(qre.tsv.qe.maxResultSize.Get()), wantfields)
}

func (qre *QueryExecutor) execStreamSQL(conn *connpool.DBConn, isTransaction bool, sql string, callback func(*sqltypes.Result) error) error {
	span, ctx := trace.NewSpan(qre.ctx, "QueryExecutor.execStreamSQL")
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	callBackClosingSpan := func(result *sqltypes.Result) error {
		defer span.Finish()
		return callback(result)
	}

	start := time.Now()
	defer qre.logStats.AddRewrittenSQL(sql, start)

	// Add query detail object into QueryExecutor TableServer list w.r.t if it is a transactional or not. Previously we were adding it
	// to olapql list regardless but that resulted in problems, where long-running stream queries which can be stateful (or transactional)
	// weren't getting cleaned up during unserveCommon>handleShutdownGracePeriod in state_manager.go.
	// This change will ensure that long-running streaming stateful queries get gracefully shutdown during ServingTypeChange
	// once their grace period is over.
	qd := NewQueryDetail(qre.logStats.Ctx, conn)
	if isTransaction {
		qre.tsv.statefulql.Add(qd)
		defer qre.tsv.statefulql.Remove(qd)
		return conn.StreamOnce(ctx, sql, callBackClosingSpan, allocStreamResult, int(qre.tsv.qe.streamBufferSize.Get()), sqltypes.IncludeFieldsOrDefault(qre.options))
	}
	qre.tsv.olapql.Add(qd)
	defer qre.tsv.olapql.Remove(qd)
	return conn.Stream(ctx, sql, callBackClosingSpan, allocStreamResult, int(qre.tsv.qe.streamBufferSize.Get()), sqltypes.IncludeFieldsOrDefault(qre.options))
}

func (qre *QueryExecutor) recordUserQuery(queryType string, duration int64) {
	username := callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(qre.ctx))
	if username == "" {
		username = callerid.GetUsername(callerid.ImmediateCallerIDFromContext(qre.ctx))
	}
	tableName := qre.plan.TableName()
	qre.tsv.Stats().UserTableQueryCount.Add([]string{tableName, username, queryType}, 1)
	qre.tsv.Stats().UserTableQueryTimesNs.Add([]string{tableName, username, queryType}, duration)
}

func generateBindVarsForViewDDLInsert(createView *sqlparser.CreateView) map[string]*querypb.BindVariable {
	bindVars := make(map[string]*querypb.BindVariable)
	bindVars["table_name"] = sqltypes.StringBindVariable(createView.ViewName.Name.String())
	bindVars["create_statement"] = sqltypes.StringBindVariable(sqlparser.String(createView))
	if createView.ViewName.Qualifier.String() != "" {
		bindVars["table_schema"] = sqltypes.StringBindVariable(createView.ViewName.Qualifier.String())
	}
	return bindVars
}

func (qre *QueryExecutor) GetSchemaDefinitions(keyspace string, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	switch tableType {
	case querypb.SchemaTableType_VIEWS:
		return qre.getViewDefinitions(keyspace, tableNames, callback)
	}
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table type %v", tableType)
}

func (qre *QueryExecutor) getViewDefinitions(keyspace string, viewNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	if keyspace == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "when getting information from mysql.views, table_schema of views is required")
	}
	bindVars := make(map[string]*querypb.BindVariable)
	bindVars["table_schema"] = sqltypes.StringBindVariable(keyspace)

	query := mysql.FetchViews
	if len(viewNames) > 0 {
		query = mysql.FetchUpdatedViews
		viewNamesBV, err := sqltypes.BuildBindVariable(viewNames)
		if err != nil {
			return err
		}
		bindVars["view_names"] = viewNamesBV
	}
	return qre.generateFinalQueryAndStreamExecute(query, bindVars, func(result *sqltypes.Result) error {
		schemaDef := make(map[string]string)
		for _, row := range result.Rows {
			schemaDef[row[0].ToString()] = row[1].ToString()
		}
		return callback(&querypb.GetSchemaResponse{TableDefinition: schemaDef})
	})
}

func (qre *QueryExecutor) generateFinalQueryAndStreamExecute(query string, bindVars map[string]*querypb.BindVariable, callback func(result *sqltypes.Result) error) error {
	sql := query
	if len(bindVars) > 0 {
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return err
		}
		sql, _, err = qre.generateFinalSQL(sqlparser.NewParsedQuery(stmt), bindVars)
		if err != nil {
			return err
		}
	}

	conn, err := qre.getStreamConn()
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return qre.execStreamSQL(conn, false /* isTransaction */, sql, callback)
}

func isInspectFilter(leadingComment string) bool {
	return strings.ReplaceAll(leadingComment, " ", "") == strings.ReplaceAll("/*explain filter*/", " ", "")
}

func (qre *QueryExecutor) getFilterInfo() (*sqltypes.Result, error) {
	var rows [][]sqltypes.Value

	for _, action := range qre.matchedActionList {
		filter := action.GetRule()
		rows = append(rows, sqltypes.BuildVarCharRow(
			filter.Name,
			filter.Description,
			strconv.Itoa(filter.Priority),
			filter.GetActionType(),
			filter.GetActionArgs(),
		))
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Name", "description", "priority", "action", "action_args"),
		Rows:   rows,
	}, nil
}
