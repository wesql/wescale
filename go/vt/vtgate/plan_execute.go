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
	"errors"
	"strings"
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/logstats"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type planExec func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, startTime time.Time) error
type txResult func(sqlparser.StatementType, *sqltypes.Result) error

func (e *Executor) newExecute(
	ctx context.Context,
	safeSession *SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	logStats *logstats.LogStats,
	execPlan planExec, // used when there is a plan to execute
	recResult txResult, // used when it's something simple like begin/commit/rollback/savepoint
) error {
	// 1: Prepare before planning and execution

	// Start an implicit transaction if necessary.
	err := e.startTxIfNecessary(ctx, safeSession)
	if err != nil {
		return err
	}

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	query, comments := sqlparser.SplitMarginComments(sql)
	stmt, reserved, err := sqlparser.Parse2(query)
	if err != nil {
		return err
	}

	vcursor, err := newVCursorImpl(safeSession, query, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, e.warnShardedOnly, e.pv)
	if err != nil {
		return err
	}

	// get type of vttablet that sql will be routed to
	err = ResolveTabletType(safeSession, vcursor, stmt, sql)
	if err != nil {
		return err
	}

	// 2: Create a plan for the query
	plan, _, err := e.getPlan(ctx, stmt, reserved, vcursor, query, comments, bindVars, safeSession, logStats)

	execStart := e.logPlanningFinished(logStats, plan)

	if err != nil {
		safeSession.ClearWarnings()
		return err
	}

	if err := interceptDMLWithoutWhereEnable(safeSession, stmt); err != nil {
		return err
	}

	rst, err := HandleDMLJobSubmit(stmt, vcursor, sql)
	if err != nil {
		return err
	}
	if rst != nil {
		return recResult(plan.Type, rst)
	}

	if plan.Type != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	// add any warnings that the planner wants to add
	for _, warning := range plan.Warnings {
		safeSession.RecordWarning(warning)
	}

	result, err := e.handleTransactions(ctx, safeSession, plan, logStats, vcursor, stmt)
	if err != nil {
		return err
	}
	if result != nil {
		return recResult(plan.Type, result)
	}

	// 3: Prepare for execution
	err = e.addNeededBindVars(ctx, plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return err
	}

	if plan.Instructions.NeedsTransaction() {
		return e.insideTransaction(ctx, safeSession, logStats,
			func() error {
				return execPlan(ctx, plan, vcursor, bindVars, execStart)
			})
	}

	return execPlan(ctx, plan, vcursor, bindVars, execStart)
}

// handleTransactions deals with transactional queries: begin, commit, rollback and savepoint management
func (e *Executor) handleTransactions(
	ctx context.Context,
	safeSession *SafeSession,
	plan *engine.Plan,
	logStats *logstats.LogStats,
	vcursor *vcursorImpl,
	stmt sqlparser.Statement,
) (*sqltypes.Result, error) {
	// We need to explicitly handle errors, and begin/commit/rollback, since these control transactions. Everything else
	// will fall through and be handled through planning
	switch plan.Type {
	case sqlparser.StmtBegin:
		qr, err := e.handleBegin(ctx, safeSession, logStats, stmt)
		return qr, err
	case sqlparser.StmtCommit:
		qr, err := e.handleCommit(ctx, safeSession, logStats)
		return qr, err
	case sqlparser.StmtRollback:
		qr, err := e.handleRollback(ctx, safeSession, logStats)
		return qr, err
	case sqlparser.StmtSavepoint:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Savepoint", logStats, func(_ string) (*sqltypes.Result, error) {
			// Safely to ignore as there is no transaction.
			return &sqltypes.Result{}, nil
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	case sqlparser.StmtSRollback:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Rollback Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	case sqlparser.StmtRelease:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Release Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return qr, err
	}
	return nil, nil
}

func (e *Executor) startTxIfNecessary(ctx context.Context, safeSession *SafeSession) error {
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) insideTransaction(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats, execPlan func() error) error {
	mustCommit := false
	if safeSession.Autocommit && !safeSession.InTransaction() {
		mustCommit = true
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return err
		}
		// The defer acts as a failsafe. If commit was successful,
		// the rollback will be a no-op.
		defer e.txConn.Rollback(ctx, safeSession) // nolint:errcheck
	}

	// The SetAutocommitable flag should be same as mustCommit.
	// If we started a transaction because of autocommit, then mustCommit
	// will be true, which means that we can autocommit. If we were already
	// in a transaction, it means that the app started it, or we are being
	// called recursively. If so, we cannot autocommit because whatever we
	// do is likely not final.
	// The control flow is such that autocommitable can only be turned on
	// at the beginning, but never after.
	safeSession.SetAutocommittable(mustCommit)

	// If we want to instantly commit the query, then there is no need to add savepoints.
	// Any partial failure of the query will be taken care by rollback.
	safeSession.SetSavepointState(!mustCommit)

	// Execute!
	err := execPlan()
	if err != nil {
		return err
	}

	if mustCommit {
		commitStart := time.Now()
		if err := e.txConn.Commit(ctx, safeSession); err != nil {
			return err
		}
		logStats.CommitTime = time.Since(commitStart)
	}
	return nil
}

func (e *Executor) executePlan(
	ctx context.Context,
	safeSession *SafeSession,
	plan *engine.Plan,
	vcursor *vcursorImpl,
	bindVars map[string]*querypb.BindVariable,
	logStats *logstats.LogStats,
	execStart time.Time,
) (*sqltypes.Result, error) {

	// 4: Execute!
	qr, err := vcursor.ExecutePrimitive(ctx, plan.Instructions, bindVars, true)

	// 5: Log and add statistics
	e.setLogStats(logStats, plan, vcursor, execStart, err, qr)

	// Check if there was partial DML execution. If so, rollback the effect of the partially executed query.
	if err != nil {
		return nil, e.rollbackExecIfNeeded(ctx, safeSession, bindVars, logStats, err)
	}
	return qr, nil
}

// rollbackExecIfNeeded rollbacks the partial execution if earlier it was detected that it needs partial query execution to be rolled back.
func (e *Executor) rollbackExecIfNeeded(ctx context.Context, safeSession *SafeSession, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats, err error) error {
	if safeSession.InTransaction() && safeSession.IsRollbackSet() {
		rErr := e.rollbackPartialExec(ctx, safeSession, bindVars, logStats)
		return vterrors.Wrap(err, rErr.Error())
	}
	return err
}

// rollbackPartialExec rollbacks to the savepoint or rollbacks transaction based on the value set on SafeSession.rollbackOnPartialExec.
// Once, it is used the variable is reset.
// If it fails to rollback to the previous savepoint then, the transaction is forced to be rolled back.
func (e *Executor) rollbackPartialExec(ctx context.Context, safeSession *SafeSession, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) error {
	var err error

	// needs to rollback only once.
	rQuery := safeSession.rollbackOnPartialExec
	if rQuery != txRollback {
		safeSession.SavepointRollback()
		_, _, err := e.execute(ctx, safeSession, rQuery, bindVars, logStats)
		if err == nil {
			return vterrors.New(vtrpcpb.Code_ABORTED, "reverted partial DML execution failure")
		}
		// not able to rollback changes of the failed query, so have to abort the complete transaction.
	}

	// abort the transaction.
	_ = e.txConn.Rollback(ctx, safeSession)
	var errMsg = "transaction rolled back to reverse changes of partial DML execution"
	if err != nil {
		return vterrors.Wrap(err, errMsg)
	}
	return vterrors.New(vtrpcpb.Code_ABORTED, errMsg)
}

func (e *Executor) setLogStats(logStats *logstats.LogStats, plan *engine.Plan, vcursor *vcursorImpl, execStart time.Time, err error, qr *sqltypes.Result) {
	logStats.StmtType = plan.Type.String()
	logStats.ActiveKeyspace = vcursor.keyspace
	logStats.TablesUsed = plan.TablesUsed
	logStats.TabletType = vcursor.TabletType().String()
	errCount := e.logExecutionEnd(logStats, execStart, plan, err, qr)
	plan.AddStats(1, time.Since(logStats.StartTime), logStats.ShardQueries, logStats.RowsAffected, logStats.RowsReturned, errCount)
}

func (e *Executor) logExecutionEnd(logStats *logstats.LogStats, execStart time.Time, plan *engine.Plan, err error, qr *sqltypes.Result) uint64 {
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
		logStats.RowsReturned = uint64(len(qr.Rows))
	}
	return errCount
}

func (e *Executor) logPlanningFinished(logStats *logstats.LogStats, plan *engine.Plan) time.Time {
	execStart := time.Now()
	if plan != nil {
		logStats.StmtType = plan.Type.String()
	}
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	return execStart
}

func interceptDMLWithoutWhereEnable(safeSession *SafeSession, stmt sqlparser.Statement) error {
	if safeSession.GetEnableInterceptionForDMLWithoutWhere() {
		switch t := stmt.(type) {
		case *sqlparser.Delete:
			if t.Where == nil {
				return errors.New("the interception of DELETE and UPDATE SQL statements without a WHERE condition is enabled. Disable this feature by inputting set session enable_interception_for_dml_without_where=false;")
			}
		case *sqlparser.Update:
			if t.Where == nil {
				return errors.New("the interception of DELETE and UPDATE SQL statements without a WHERE condition is enabled. Disable this feature by inputting set session enable_interception_for_dml_without_where=false;")
			}
		}
	}
	return nil
}

func InitResolverOptionsIfNil(safeSession *SafeSession) {
	if safeSession.ResolverOptions == nil {
		safeSession.ResolverOptions = &vtgatepb.ResolverOptions{
			ReadWriteSplittingRatio: int32(defaultReadWriteSplittingRatio),
			KeyspaceTabletType:      topodatapb.TabletType_UNKNOWN,
			UserHintTabletType:      topodatapb.TabletType_UNKNOWN,
			SuggestedTabletType:     topodatapb.TabletType_UNKNOWN,
		}
	}
}

func GetSuggestedTabletType(safeSession *SafeSession, sql string) (topodatapb.TabletType, error) {
	isReadOnlyTx := safeSession.Session.InTransaction && safeSession.Session.TransactionAccessMode == vtgatepb.TransactionAccessMode_READ_ONLY
	// Users can input "set session/global enable_read_write_splitting_for_read_only_txn=true/false" to enable or disable read-only transaction routing.
	// The user-input value is save in the session.ReadWriteSplitForReadOnlyTxnUserInput field.
	// However, if the user enables or disables this feature while already in a read-only transaction, immediate efficiency is not applied.
	// Instead, the user input takes effect only when not in a read-only transaction.
	// Here, EnableReadWriteSplitForReadOnlyTxn serves as a true switch for this feature, and it will equal to the user input only when not in a read-only transaction.
	// Here is an example to help understanding:
	// -------------------------------
	// user enable/disable this feature --- 1
	// user start read only txn
	// SQLs...
	// user enable/disable this feature --- 2
	// other SQLs...
	// user enable/disable this feature --- 3
	// user commit
	// -------------------------------
	// During the txn, value set in 1 works. After committing, value set in 3 works.

	if !isReadOnlyTx {
		safeSession.Session.EnableReadWriteSplitForReadOnlyTxn = safeSession.Session.GetReadWriteSplitForReadOnlyTxnUserInput()
	}

	// use the suggestedTabletType if safeSession.TargetString is not specified
	suggestedTabletType, err := suggestTabletType(safeSession.GetReadWriteSplittingPolicy(), safeSession.InTransaction(),
		safeSession.HasCreatedTempTables(), safeSession.HasAdvisoryLock(), safeSession.GetReadWriteSplittingRatio(), sql, safeSession.GetEnableReadWriteSplitForReadOnlyTxn(), isReadOnlyTx)
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}

	return suggestedTabletType, nil
}

func GetKeyspaceTabletType(safeSession *SafeSession) (topodatapb.TabletType, error) {
	// get keyspace tablet type, like: use d1@primary
	last := strings.LastIndexAny(safeSession.TargetString, "@")
	if last != -1 {
		return topoprotopb.ParseTabletType(safeSession.TargetString[last+1:])
	}
	return topodatapb.TabletType_UNKNOWN, nil
}

func GetUserHintTabletType(stmt sqlparser.Statement, vcursor *vcursorImpl) (topodatapb.TabletType, error) {
	tabletTypeFromHint := sqlparser.GetNodeType(stmt)
	if tabletTypeFromHint == topodatapb.TabletType_PRIMARY || tabletTypeFromHint == topodatapb.TabletType_REPLICA || tabletTypeFromHint == topodatapb.TabletType_RDONLY {
		err := vcursor.CheckTabletTypeFromHint(tabletTypeFromHint)
		if err != nil {
			return topodatapb.TabletType_UNKNOWN, err
		}
		return tabletTypeFromHint, nil
	}
	return topodatapb.TabletType_UNKNOWN, nil
}

func ResolveTabletType(safeSession *SafeSession, vcursor *vcursorImpl, stmt sqlparser.Statement, sql string) error {
	// init ResolverOptions if nil
	InitResolverOptionsIfNil(safeSession)

	// get UserHintTabletType
	var err error
	safeSession.ResolverOptions.UserHintTabletType, err = GetUserHintTabletType(stmt, vcursor)
	if err != nil {
		return err
	}
	if safeSession.ResolverOptions.UserHintTabletType != topodatapb.TabletType_UNKNOWN {
		vcursor.tabletType = safeSession.ResolverOptions.UserHintTabletType
		return nil
	}

	// get KeyspaceTabletType
	safeSession.ResolverOptions.KeyspaceTabletType, err = GetKeyspaceTabletType(safeSession)
	if err != nil {
		return err
	}
	if safeSession.ResolverOptions.KeyspaceTabletType != topodatapb.TabletType_UNKNOWN {
		vcursor.tabletType = safeSession.ResolverOptions.KeyspaceTabletType
		return nil
	}

	// get SuggestedTabletType
	safeSession.ResolverOptions.SuggestedTabletType, err = GetSuggestedTabletType(safeSession, sql)
	if err != nil {
		return err
	}
	vcursor.tabletType = safeSession.ResolverOptions.SuggestedTabletType
	return nil
}

func IsSubmitDMLJob(stmt sqlparser.Statement) bool {
	cmd := sqlparser.GetDMLJobCmd(stmt)
	return cmd == "true"
}

func HandleDMLJobSubmit(stmt sqlparser.Statement, vcursor *vcursorImpl, sql string) (*sqltypes.Result, error) {
	if IsSubmitDMLJob(stmt) {
		// if in transaction, return error
		if vcursor.InTransaction() {
			return nil, errors.New("cannot submit DML job in transaction")
		}

		timeGapInMs, batchSize, postponeLaunch, failPolicy, timePeriodStart, timePeriodEnd, timePeriodTimeZone, throttleDuration, throttleRatio := sqlparser.GetDMLJobArgs(stmt)
		qr, err := vcursor.executor.SubmitDMLJob("submit_job", sql, "", vcursor.keyspace, timePeriodStart, timePeriodEnd, timePeriodTimeZone, timeGapInMs, batchSize, postponeLaunch, failPolicy, throttleDuration, throttleRatio)
		if qr != nil {
			if qr.RowsAffected == 1 {
				qr.Info = "job submitted successfully"
			} else {
				qr.Info = "job submitted failed"
			}
		}
		return qr, err
	}
	return nil, nil
}
