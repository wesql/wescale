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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"

	"vitess.io/vitess/go/internal/global"

	"github.com/pingcap/failpoint"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	"vitess.io/vitess/go/vt/failpointkey"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/discovery"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	errNoKeyspace     = vterrors.VT09005()
	defaultTabletType = topodatapb.TabletType_PRIMARY

	// TODO: @rafael - These two counters should be deprecated in favor of the ByTable ones in v17+. They are kept for now for backwards compatibility.
	queriesProcessed = stats.NewCountersWithSingleLabel("QueriesProcessed", "Queries processed at vtgate by plan type", "Plan")
	queriesRouted    = stats.NewCountersWithSingleLabel("QueriesRouted", "Queries routed from vtgate to vttablet by plan type", "Plan")

	queriesProcessedByTable = stats.NewCountersWithMultiLabels("QueriesProcessedByTable", "Queries processed at vtgate by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
	queriesRoutedByTable    = stats.NewCountersWithMultiLabels("QueriesRoutedByTable", "Queries routed from vtgate to vttablet by plan type, keyspace and table", []string{"Plan", "Keyspace", "Table"})
)

const (
	bindVarPrefix = "__vt"
)

func init() {
	registerTabletTypeFlag := func(fs *pflag.FlagSet) {
		fs.Var((*topoproto.TabletTypeFlag)(&defaultTabletType), "default_tablet_type", "The default tablet type to set for queries, when one is not explicitly selected.")
	}

	servenv.OnParseFor("vtgate", registerTabletTypeFlag)
	servenv.OnParseFor("vtgateclienttest", registerTabletTypeFlag)
	servenv.OnParseFor("vtcombo", registerTabletTypeFlag)
	servenv.OnParseFor("vtexplain", registerTabletTypeFlag)
}

// Executor is the engine that executes queries by utilizing
// the abilities of the underlying vttablets.
type Executor struct {
	serv        srvtopo.Server
	cell        string
	resolver    *Resolver
	scatterConn *ScatterConn
	txConn      *TxConn
	pv          plancontext.PlannerVersion

	mu           sync.Mutex
	vschema      *vindexes.VSchema
	streamSize   int
	plans        cache.Cache
	vschemaStats *VSchemaStats

	normalize       bool
	warnShardedOnly bool

	vm            *VSchemaManager
	schemaTracker SchemaInfo

	// allowScatter will fail planning if set to false and a plan contains any scatter queries
	allowScatter bool
}

var executorOnce sync.Once

const pathQueryPlans = "/debug/query_plans"
const pathScatterStats = "/debug/scatter_stats"
const pathVSchema = "/debug/vschema"

// NewExecutor creates a new Executor.
func NewExecutor(
	ctx context.Context,
	serv srvtopo.Server,
	cell string,
	resolver *Resolver,
	normalize, warnOnShardedOnly bool,
	streamSize int,
	cacheCfg *cache.Config,
	schemaTracker SchemaInfo,
	noScatter bool,
	pv plancontext.PlannerVersion,
) *Executor {
	e := &Executor{
		serv:            serv,
		cell:            cell,
		resolver:        resolver,
		scatterConn:     resolver.scatterConn,
		txConn:          resolver.scatterConn.txConn,
		plans:           cache.NewDefaultCacheImpl(cacheCfg),
		normalize:       normalize,
		warnShardedOnly: warnOnShardedOnly,
		streamSize:      streamSize,
		schemaTracker:   schemaTracker,
		allowScatter:    !noScatter,
		pv:              pv,
	}

	vschemaacl.Init()
	// we subscribe to update from the VSchemaManager
	e.vm = &VSchemaManager{
		subscriber: e.SaveVSchema,
		serv:       serv,
		cell:       cell,
		schema:     e.schemaTracker,
	}
	serv.WatchSrvVSchema(ctx, cell, e.vm.VSchemaUpdate)

	executorOnce.Do(func() {
		stats.NewGaugeFunc("QueryPlanCacheLength", "Query plan cache length", func() int64 {
			return int64(e.plans.Len())
		})
		stats.NewGaugeFunc("QueryPlanCacheSize", "Query plan cache size", func() int64 {
			return e.plans.UsedCapacity()
		})
		stats.NewGaugeFunc("QueryPlanCacheCapacity", "Query plan cache capacity", func() int64 {
			return e.plans.MaxCapacity()
		})
		stats.NewCounterFunc("QueryPlanCacheEvictions", "Query plan cache evictions", func() int64 {
			return e.plans.Evictions()
		})
		stats.NewCounterFunc("QueryPlanCacheHits", "Query plan cache hits", func() int64 {
			return e.plans.Hits()
		})
		stats.NewCounterFunc("QueryPlanCacheMisses", "Query plan cache misses", func() int64 {
			return e.plans.Misses()
		})
		http.Handle(pathQueryPlans, e)
		http.Handle(pathScatterStats, e)
		http.Handle(pathVSchema, e)
	})
	return e
}

// Execute executes a non-streaming query.
func (e *Executor) Execute(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (result *sqltypes.Result, err error) {
	span, ctx := trace.NewSpan(ctx, "executor.Execute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	stmtType, result, err := e.execute(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err
	if result == nil {
		saveSessionStats(safeSession, stmtType, 0, 0, 0, err)
	} else {
		saveSessionStats(safeSession, stmtType, result.RowsAffected, result.InsertID, len(result.Rows), err)
	}
	if result != nil && len(result.Rows) > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := sqlparser.RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v", piiSafeSQL, warnMemoryRows)
	}

	logStats.SaveEndTime()
	QueryLogger.Send(logStats)
	return result, err
}

type streaminResultReceiver struct {
	mu           sync.Mutex
	stmtType     sqlparser.StatementType
	rowsAffected uint64
	rowsReturned int
	insertID     uint64
	callback     func(*sqltypes.Result) error
}

func (s *streaminResultReceiver) storeResultStats(typ sqlparser.StatementType, qr *sqltypes.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rowsAffected += qr.RowsAffected
	s.rowsReturned += len(qr.Rows)
	if qr.InsertID != 0 {
		s.insertID = qr.InsertID
	}
	s.stmtType = typ
	return s.callback(qr)
}

// StreamExecute executes a streaming query.
func (e *Executor) StreamExecute(
	ctx context.Context,
	method string,
	safeSession *SafeSession,
	sql string,
	bindVars map[string]*querypb.BindVariable,
	callback func(*sqltypes.Result) error,
) error {
	span, ctx := trace.NewSpan(ctx, "executor.StreamExecute")
	span.Annotate("method", method)
	trace.AnnotateSQL(span, sqlparser.Preview(sql))
	defer span.Finish()

	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	srr := &streaminResultReceiver{callback: callback}
	var err error

	resultHandler := func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) error {
		var seenResults sync2.AtomicBool
		var resultMu sync.Mutex
		result := &sqltypes.Result{}
		if canReturnRows(plan.Type) {
			srr.callback = func(qr *sqltypes.Result) error {
				resultMu.Lock()
				defer resultMu.Unlock()
				// If the row has field info, send it separately.
				// TODO(sougou): this behavior is for handling tests because
				// the framework currently sends all results as one packet.
				byteCount := 0
				if len(qr.Fields) > 0 {
					if err := callback(qr.Metadata()); err != nil {
						return err
					}
					seenResults.Set(true)
				}

				for _, row := range qr.Rows {
					result.Rows = append(result.Rows, row)

					for _, col := range row {
						byteCount += col.Len()
					}

					if byteCount >= e.streamSize {
						err := callback(result)
						seenResults.Set(true)
						result = &sqltypes.Result{}
						byteCount = 0
						if err != nil {
							return err
						}
					}
				}
				return nil
			}
		}

		// 4: Execute!
		err := vc.StreamExecutePrimitive(ctx, plan.Instructions, bindVars, true, func(qr *sqltypes.Result) error {
			return srr.storeResultStats(plan.Type, qr)
		})

		// Check if there was partial DML execution. If so, rollback the effect of the partially executed query.
		if err != nil {
			if !canReturnRows(plan.Type) {
				return e.rollbackExecIfNeeded(ctx, safeSession, bindVars, logStats, err)
			}
			return err
		}

		if !canReturnRows(plan.Type) {
			return nil
		}

		// Send left-over rows if there is no error on execution.
		if len(result.Rows) > 0 || !seenResults.Get() {
			if err := callback(result); err != nil {
				return err
			}
		}

		// 5: Log and add statistics
		logStats.TablesUsed = plan.TablesUsed
		logStats.TabletType = vc.TabletType().String()
		logStats.ExecuteTime = time.Since(execStart)
		logStats.ActiveKeyspace = vc.keyspace

		e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

		return err
	}

	err = e.newExecute(ctx, safeSession, sql, bindVars, logStats, resultHandler, srr.storeResultStats)

	logStats.Error = err
	saveSessionStats(safeSession, srr.stmtType, srr.rowsAffected, srr.insertID, srr.rowsReturned, err)
	if srr.rowsReturned > warnMemoryRows {
		warnings.Add("ResultsExceeded", 1)
		piiSafeSQL, err := sqlparser.RedactSQLQuery(sql)
		if err != nil {
			piiSafeSQL = logStats.StmtType
		}
		log.Warningf("%q exceeds warning threshold of max memory rows: %v", piiSafeSQL, warnMemoryRows)
	}

	logStats.SaveEndTime()
	QueryLogger.Send(logStats)
	return err

}

func canReturnRows(stmtType sqlparser.StatementType) bool {
	switch stmtType {
	case sqlparser.StmtSelect, sqlparser.StmtShow, sqlparser.StmtExplain, sqlparser.StmtCallProc:
		return true
	default:
		return false
	}
}

// NewJaegerSpanContext returns a new Jaeger span context(https://www.jaegertracing.io/docs/1.48/client-libraries/).
func NewJaegerSpanContext() string {
	// fake jaeger config
	cfg := &config.Configuration{
		ServiceName: "mock-upstream-service-name",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		fmt.Printf("Failed to create tracer: %v", err)
		return ""
	}
	defer closer.Close()
	span := tracer.StartSpan("fake-span")
	defer span.Finish()
	rand.Seed(time.Now().UnixNano())
	// fake span id as random uint64
	encodedSpanID := strconv.FormatUint(rand.Uint64(), 16)
	ctx := span.Context().(jaeger.SpanContext)
	// fake tracer id as random 128 bits
	encodedUberTraceID := fmt.Sprintf("%x", ctx.TraceID())
	// fake parent span id as 0
	encodedParentSpanID := strconv.FormatUint(0, 16)
	// fake flags as 0x07
	encodedFlags := strconv.FormatUint(7, 16)
	traceIDValue := fmt.Sprintf("%v:%v:%v:%v", encodedUberTraceID, encodedSpanID, encodedParentSpanID, encodedFlags)
	traceIDKey := "uber-trace-id"
	header := make(map[string]string)
	header[traceIDKey] = traceIDValue
	jsonData, err := json.Marshal(header)
	if err != nil {
		fmt.Println("JSON marshaling failed:", err)
		return ""
	}
	base64Data := base64.StdEncoding.EncodeToString(jsonData)
	traceInfo := fmt.Sprintf("/*TRACEID=%s*/ /*VT_SPAN_CONTEXT=%s*/", encodedUberTraceID, base64Data)
	return traceInfo
}

func saveSessionStats(safeSession *SafeSession, stmtType sqlparser.StatementType, rowsAffected, insertID uint64, rowsReturned int, err error) {
	safeSession.RowCount = -1
	if err != nil {
		return
	}
	if !safeSession.foundRowsHandled {
		safeSession.FoundRows = uint64(rowsReturned)
	}
	if insertID > 0 {
		safeSession.LastInsertId = insertID
	}
	switch stmtType {
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		safeSession.RowCount = int64(rowsAffected)
	case sqlparser.StmtDDL, sqlparser.StmtSet, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtFlush:
		safeSession.RowCount = 0
	}
}

func (e *Executor) execute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	var stmtType sqlparser.StatementType
	err = e.newExecute(ctx, safeSession, sql, bindVars, logStats, func(ctx context.Context, plan *engine.Plan, vc *vcursorImpl, bindVars map[string]*querypb.BindVariable, time time.Time) error {
		stmtType = plan.Type
		qr, err = e.executePlan(ctx, safeSession, plan, vc, bindVars, logStats, time)
		return err
	}, func(typ sqlparser.StatementType, result *sqltypes.Result) error {
		stmtType = typ
		qr = result
		return nil
	})

	return stmtType, qr, err
}

// addNeededBindVars adds bind vars that are needed by the plan
func (e *Executor) addNeededBindVars(ctx context.Context, bindVarNeeds *sqlparser.BindVarNeeds, bindVars map[string]*querypb.BindVariable, session *SafeSession) error {
	udvMap := session.UserDefinedVariables
	if udvMap == nil {
		udvMap = map[string]*querypb.BindVariable{}
	}

	for _, funcName := range bindVarNeeds.NeedFunctionResult {
		switch funcName {
		case sqlparser.DBVarName:
			bindVars[sqlparser.DBVarName] = sqltypes.StringBindVariable(session.TargetString)
		case sqlparser.LastInsertIDName:
			bindVars[sqlparser.LastInsertIDName] = sqltypes.Uint64BindVariable(session.GetLastInsertId())
		case sqlparser.FoundRowsName:
			bindVars[sqlparser.FoundRowsName] = sqltypes.Int64BindVariable(int64(session.FoundRows))
		case sqlparser.RowCountName:
			bindVars[sqlparser.RowCountName] = sqltypes.Int64BindVariable(session.RowCount)
		case sqlparser.JaegerSpanContextName:
			bindVars[sqlparser.JaegerSpanContextName] = sqltypes.StringBindVariable(NewJaegerSpanContext())
		case sqlparser.CurrentUserName:
			//bindVars[sqlparser.CurrentUserName] = sqltypes.Int64BindVariable(session.RowCount)
			im := callerid.ImmediateCallerIDFromContext(ctx)
			userAndHost := im.GetUsername() + "@" + im.GetHost()
			bindVars[sqlparser.CurrentUserName] = sqltypes.StringBindVariable(userAndHost)
		case sqlparser.InternalInfo:
			key := udvMap["internal_info_key"]
			if key == nil {
				return errors.New("internal_info_key is not set")
			}
			res, err := GetInternalInfo(string(key.Value), e)
			if err != nil {
				return err
			}
			bindVars[sqlparser.InternalInfo] = sqltypes.StringBindVariable(res)
		}
	}

	for _, sysVar := range bindVarNeeds.NeedSystemVariable {
		key := bindVarPrefix + sysVar
		switch sysVar {
		case sysvars.Autocommit.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.Autocommit)
		case sysvars.QueryTimeout.Name:
			bindVars[key] = sqltypes.Int64BindVariable(session.GetQueryTimeout())
		case sysvars.ClientFoundRows.Name:
			var v bool
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.ClientFoundRows
			})
			bindVars[key] = sqltypes.BoolBindVariable(v)
		case sysvars.SkipQueryPlanCache.Name:
			var v bool
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.ClientFoundRows
			})
			bindVars[key] = sqltypes.BoolBindVariable(v)
		case sysvars.SQLSelectLimit.Name:
			var v int64
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.SqlSelectLimit
			})
			bindVars[key] = sqltypes.Int64BindVariable(v)
		case sysvars.TransactionMode.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.TransactionMode.String())
		case sysvars.Workload.Name:
			var v string
			ifOptionsExist(session, func(options *querypb.ExecuteOptions) {
				v = options.GetWorkload().String()
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.DDLStrategy.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.DDLStrategy)
		case sysvars.EnableDeclarativeDDL.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.EnableDeclarativeDDL)
		case sysvars.ReadWriteSplittingPolicy.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.ReadWriteSplittingPolicy)
		case sysvars.ReadWriteSplittingRatio.Name:
			bindVars[key] = sqltypes.Int32BindVariable(session.ReadWriteSplittingRatio)
		case sysvars.EnableInterceptionForDMLWithoutWhere.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.EnableInterceptionForDMLWithoutWhere)
		case sysvars.EnableDisplaySQLExecutionVTTablet.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.EnableDisplaySQLExecutionVTTabletType)
		case sysvars.ReadWriteSplitForReadOnlyTxnUserInput.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.ReadWriteSplitForReadOnlyTxnUserInput)
		case sysvars.RewriteTableNameWithDbNamePrefix.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.RewriteTableNameWithDbNamePrefix)
		case sysvars.SessionUUID.Name:
			bindVars[key] = sqltypes.StringBindVariable(session.SessionUUID)
		case sysvars.SessionEnableSystemSettings.Name:
			bindVars[key] = sqltypes.BoolBindVariable(session.EnableSystemSettings)
		case sysvars.ReadAfterWriteGTID.Name:
			var v string
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				v = raw.ReadAfterWriteGtid
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.ReadAfterWriteConsistency.Name:
			var v string
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				v = raw.ReadAfterWriteConsistency.String()
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.ReadAfterWriteTimeOut.Name:
			var v float64
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				v = raw.ReadAfterWriteTimeout
			})
			bindVars[key] = sqltypes.Float64BindVariable(v)
		case sysvars.SessionTrackGTIDs.Name:
			v := "off"
			ifReadAfterWriteExist(session, func(raw *vtgatepb.ReadAfterWrite) {
				if raw.SessionTrackGtids {
					v = "own_gtid"
				}
			})
			bindVars[key] = sqltypes.StringBindVariable(v)
		case sysvars.Version.Name:
			bindVars[key] = sqltypes.StringBindVariable(servenv.AppVersion.MySQLVersion())
		case sysvars.VersionComment.Name:
			bindVars[key] = sqltypes.StringBindVariable(servenv.AppVersion.String())
		case sysvars.Socket.Name:
			bindVars[key] = sqltypes.StringBindVariable(mysqlSocketPath())
		default:
			if value, hasSysVar := session.SystemVariables[sysVar]; hasSysVar {
				expr, err := sqlparser.ParseExpr(value)
				if err != nil {
					return err
				}

				evalExpr, err := evalengine.Translate(expr, nil)
				if err != nil {
					return err
				}
				evaluated, err := evalengine.EmptyExpressionEnv().Evaluate(evalExpr)
				if err != nil {
					return err
				}
				bindVars[key] = sqltypes.ValueBindVariable(evaluated.Value())
			}
		}
	}

	for _, udv := range bindVarNeeds.NeedUserDefinedVariables {
		val := udvMap[udv]
		if val == nil {
			val = sqltypes.NullBindVariable
		}
		bindVars[sqlparser.UserDefinedVariableName+udv] = val
	}

	return nil
}

func ifOptionsExist(session *SafeSession, f func(*querypb.ExecuteOptions)) {
	options := session.GetOptions()
	if options != nil {
		f(options)
	}
}

func ifReadAfterWriteExist(session *SafeSession, f func(*vtgatepb.ReadAfterWrite)) {
	raw := session.ReadAfterWrite
	if raw != nil {
		f(raw)
	}
}

func (e *Executor) handleBegin(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats, stmt sqlparser.Statement) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	begin := stmt.(*sqlparser.Begin)
	err := e.txConn.Begin(ctx, safeSession, begin.TxAccessModes)

	// verify read only tx
	// when start transaction with READ_ONLY and WithConsistentSnapshot at the same time, the database should ensure that the consistent snapshopt can work correctly between primary and replicate nodes
	if (len(begin.TxAccessModes) == 1 && begin.TxAccessModes[0] == sqlparser.ReadOnly) ||
		(len(begin.TxAccessModes) == 2 && begin.TxAccessModes[0] == sqlparser.ReadOnly && begin.TxAccessModes[1] == sqlparser.WithConsistentSnapshot) ||
		(len(begin.TxAccessModes) == 2 && begin.TxAccessModes[1] == sqlparser.ReadOnly && begin.TxAccessModes[0] == sqlparser.WithConsistentSnapshot) {
		safeSession.Session.TransactionAccessMode = vtgatepb.TransactionAccessMode_READ_ONLY
	}

	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts("Begin", "", "", 0)

	return &sqltypes.Result{}, err
}

func (e *Executor) handleCommit(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts("Commit", "", "", int64(logStats.ShardQueries))

	err := e.txConn.Commit(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

// Commit commits the existing transactions
func (e *Executor) Commit(ctx context.Context, safeSession *SafeSession) error {
	return e.txConn.Commit(ctx, safeSession)
}

func (e *Executor) handleRollback(ctx context.Context, safeSession *SafeSession, logStats *logstats.LogStats) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts("Rollback", "", "", int64(logStats.ShardQueries))
	err := e.txConn.Rollback(ctx, safeSession)
	logStats.CommitTime = time.Since(execStart)
	return &sqltypes.Result{}, err
}

func (e *Executor) handleSavepoint(ctx context.Context, safeSession *SafeSession, sql string, planType string, logStats *logstats.LogStats, nonTxResponse func(query string) (*sqltypes.Result, error), ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	logStats.ShardQueries = uint64(len(safeSession.ShardSessions))
	e.updateQueryCounts(planType, "", "", int64(logStats.ShardQueries))
	defer func() {
		logStats.ExecuteTime = time.Since(execStart)
	}()

	// If no transaction exists on any of the shard sessions,
	// then savepoint does not need to be executed, it will be only stored in the session
	// and later will be executed when a transaction is started.
	if !safeSession.isTxOpen() {
		if safeSession.InTransaction() {
			// Storing, as this needs to be executed just after starting transaction on the shard.
			safeSession.StoreSavepoint(sql)
			return &sqltypes.Result{}, nil
		}
		return nonTxResponse(sql)
	}
	orig := safeSession.commitOrder
	qr, err := e.executeSPInAllSessions(ctx, safeSession, sql, ignoreMaxMemoryRows)
	safeSession.SetCommitOrder(orig)
	if err != nil {
		return nil, err
	}
	safeSession.StoreSavepoint(sql)
	return qr, nil
}

// executeSPInAllSessions function executes the savepoint query in all open shard sessions (pre, normal and post)
// which has non-zero transaction id (i.e. an open transaction on the shard connection).
func (e *Executor) executeSPInAllSessions(ctx context.Context, safeSession *SafeSession, sql string, ignoreMaxMemoryRows bool) (*sqltypes.Result, error) {
	var qr *sqltypes.Result
	var errs []error
	for _, co := range []vtgatepb.CommitOrder{vtgatepb.CommitOrder_PRE, vtgatepb.CommitOrder_NORMAL, vtgatepb.CommitOrder_POST} {
		safeSession.SetCommitOrder(co)

		var rss []*srvtopo.ResolvedShard
		var queries []*querypb.BoundQuery
		for _, shardSession := range safeSession.getSessions() {
			// This will avoid executing savepoint on reserved connections
			// which has no open transaction.
			if shardSession.TransactionId == 0 {
				continue
			}
			rss = append(rss, &srvtopo.ResolvedShard{
				Target:  shardSession.Target,
				Gateway: e.resolver.resolver.GetGateway(),
			})
			queries = append(queries, &querypb.BoundQuery{Sql: sql})
		}
		qr, errs = e.ExecuteMultiShard(ctx, nil, rss, queries, safeSession, false /*autocommit*/, ignoreMaxMemoryRows)
		err := vterrors.Aggregate(errs)
		if err != nil {
			return nil, err
		}
	}
	return qr, nil
}

// CloseSession releases the current connection, which rollbacks open transactions and closes reserved connections.
// It is called then the MySQL servers closes the connection to its client.
func (e *Executor) CloseSession(ctx context.Context, safeSession *SafeSession) error {
	return e.txConn.ReleaseAll(ctx, safeSession)
}

func (e *Executor) setVitessMetadata(ctx context.Context, name, value string) error {
	// TODO(kalfonso): move to its own acl check and consolidate into an acl component that can handle multiple operations (vschema, metadata)
	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' not authorized to perform vitess metadata operations", user.GetUsername())
	}

	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return err
	}

	if value == "" {
		return ts.DeleteMetadata(ctx, name)
	}
	return ts.UpsertMetadata(ctx, name, value)
}

func (e *Executor) showVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if filter == nil {
		metadata, err = ts.GetMetadata(ctx, "")
		if err != nil {
			return nil, err
		}
	} else {
		metadata, err = ts.GetMetadata(ctx, filter.Like)
		if err != nil {
			return nil, err
		}
	}

	rows := make([][]sqltypes.Value, 0, len(metadata))
	for k, v := range metadata {
		row := sqltypes.BuildVarCharRow(k, v)
		rows = append(rows, row)
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Key", "Value"),
		Rows:   rows,
	}, nil
}

type tabletFilter func(tablet *topodatapb.Tablet, servingState string, primaryTermStartTime int64) bool

func (e *Executor) showShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error) {
	showVitessShardsFilters := func(filter *sqlparser.ShowFilter) ([]func(string) bool, []func(string, *topodatapb.ShardReference) bool) {
		keyspaceFilters := []func(string) bool{}
		shardFilters := []func(string, *topodatapb.ShardReference) bool{}

		if filter == nil {
			return keyspaceFilters, shardFilters
		}

		if filter.Like != "" {
			shardLikeRexep := sqlparser.LikeToRegexp(filter.Like)

			if strings.Contains(filter.Like, "/") {
				keyspaceLikeRexep := sqlparser.LikeToRegexp(strings.Split(filter.Like, "/")[0])
				keyspaceFilters = append(keyspaceFilters, func(ks string) bool {
					return keyspaceLikeRexep.MatchString(ks)
				})
			}
			shardFilters = append(shardFilters, func(ks string, shard *topodatapb.ShardReference) bool {
				return shardLikeRexep.MatchString(topoproto.KeyspaceShardString(ks, shard.Name))
			})

			return keyspaceFilters, shardFilters
		}

		if filter.Filter != nil {
			// TODO build a query planner I guess? lol that should be fun
			log.Infof("SHOW VITESS_SHARDS where clause %+v. Ignoring this (for now).", filter.Filter)
		}

		return keyspaceFilters, shardFilters
	}

	keyspaceFilters, shardFilters := showVitessShardsFilters(filter)

	keyspaces, err := e.resolver.resolver.GetAllKeyspaces(ctx)
	if err != nil {
		return nil, err
	}

	var rows [][]sqltypes.Value
	for _, keyspace := range keyspaces {
		skipKeyspace := false
		for _, filter := range keyspaceFilters {
			if !filter(keyspace) {
				skipKeyspace = true
				break
			}
		}

		if skipKeyspace {
			continue
		}

		_, _, shards, err := e.resolver.resolver.GetKeyspaceShards(ctx, keyspace, destTabletType)
		if err != nil {
			// There might be a misconfigured keyspace or no shards in the keyspace.
			// Skip any errors and move on.
			continue
		}

		for _, shard := range shards {
			skipShard := false
			for _, filter := range shardFilters {
				if !filter(keyspace, shard) {
					skipShard = true
					break
				}
			}

			if skipShard {
				continue
			}

			rows = append(rows, sqltypes.BuildVarCharRow(topoproto.KeyspaceShardString(keyspace, shard.Name)))
		}
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Shards"),
		Rows:   rows,
	}, nil
}

func (e *Executor) showLastSeenGTID(_ *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	rows := [][]sqltypes.Value{}
	lastSeenGTID := e.scatterConn.gateway.lastSeenGtid.String()
	rows = append(rows, sqltypes.BuildVarCharRow(lastSeenGTID))
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("LastSeenGTID"),
		Rows:   rows,
	}, nil
}
func (e *Executor) showFailPoint(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	boolToString := func(b bool) string {
		if b {
			return "true"
		}
		return "false"
	}
	rows := [][]sqltypes.Value{}
	// first show failpoint in static map
	for key := range failpointkey.FailpointTable {
		Enabled := false
		_, err := failpoint.Status(key)
		if err == nil {
			Enabled = true
		}
		rows = append(rows, sqltypes.BuildVarCharRow(key, boolToString(Enabled), failpointkey.FailpointTable[key].ExampleStr))
	}
	// then show failpoint not in static map but in failpoint.List()
	for _, key := range failpoint.List() {
		if _, isExist := failpointkey.FailpointTable[key]; !isExist {
			Enabled := false
			_, err := failpoint.Status(key)
			if err == nil {
				Enabled = true
			}
			rows = append(rows, sqltypes.BuildVarCharRow(key, boolToString(Enabled), failpointkey.FailpointTable[key].ExampleStr))
		}
	}
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("failpoint keys", "Enabled", "Example"),
		Rows:   rows,
		Info:   "\nTo enable failpoint: set @put_failpoint='key=value'\nTo disable failpoint: set @remove_failpoint='key'",
	}, nil
}

func (e *Executor) showTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	getTabletFilters := func(filter *sqlparser.ShowFilter) []tabletFilter {
		var filters []tabletFilter

		if filter == nil {
			return filters
		}

		if filter.Like != "" {
			tabletRegexp := sqlparser.LikeToRegexp(filter.Like)

			f := func(tablet *topodatapb.Tablet, servingState string, primaryTermStartTime int64) bool {
				return tabletRegexp.MatchString(tablet.Hostname)
			}

			filters = append(filters, f)
			return filters
		}

		if filter.Filter != nil {
			log.Infof("SHOW VITESS_TABLETS where clause: %+v. Ignoring this (for now).", filter.Filter)
		}

		return filters
	}

	tabletFilters := getTabletFilters(filter)

	rows := [][]sqltypes.Value{}
	status := e.scatterConn.GetHealthCheckCacheStatus()
	for _, s := range status {
		for _, ts := range s.TabletsStats {
			state := "SERVING"
			if !ts.Serving {
				state = "NOT_SERVING"
			}
			ptst := ts.PrimaryTermStartTime
			ptstStr := ""
			if ptst > 0 {
				// this code depends on the fact that PrimaryTermStartTime is the seconds since epoch start
				ptstStr = time.Unix(ptst, 0).UTC().Format(time.RFC3339)
			}

			skipTablet := false
			for _, filter := range tabletFilters {
				if !filter(ts.Tablet, state, ptst) {
					skipTablet = true
					break
				}
			}

			if skipTablet {
				continue
			}

			rows = append(rows, sqltypes.BuildVarCharRow(
				s.Cell,
				s.Target.Keyspace,
				s.Target.Shard,
				ts.Target.TabletType.String(),
				state,
				topoproto.TabletAliasString(ts.Tablet.Alias),
				ts.Tablet.Hostname,
				ptstStr,
			))
		}
	}
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Cell", "Keyspace", "Shard", "TabletType", "State", "Alias", "Hostname", "PrimaryTermStartTime"),
		Rows:   rows,
	}, nil
}

func formatTabletAlias(alias *topodatapb.TabletAlias) string {
	return fmt.Sprintf("%v-%v", alias.Cell, alias.Uid)
}

// where clause is not fully supported,
// only support "alias = xxx",
// other cases will return an ERROR.
func matchTabletByAlias(filter *sqlparser.ShowFilter, alias string) (bool, error) {
	if filter == nil {
		return true, nil
	}

	if filter.Like != "" {
		tabletRegexp := sqlparser.LikeToRegexp(filter.Like)
		return tabletRegexp.MatchString(alias), nil
	}

	if filter.Filter == nil {
		// it means that the filter clause is like ''
		return false, nil
	}

	if cmp, ok := filter.Filter.(*sqlparser.ComparisonExpr); ok {
		if cmp.Operator == sqlparser.EqualOp {
			if colName, ok := cmp.Left.(*sqlparser.ColName); ok {
				colNameLower := strings.ToLower(colName.Name.String())
				if colNameLower == "alias" {
					val := sqlparser.String(cmp.Right)
					val = strings.ReplaceAll(val, "'", "")
					val = strings.ReplaceAll(val, " ", "")
					return val == alias, nil
				}
			}
		}
	}
	return false, fmt.Errorf("where caluse is not fully supported now, only support alias =\"xxx\"")
}

func (e *Executor) showTabletsPlans(filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	rows := make([]sqltypes.Row, 0)
	for _, tabletStatusList := range e.scatterConn.GetHealthCheckCacheStatus() {
		for _, tabletStatus := range tabletStatusList.TabletsStats {

			tabletAlias := formatTabletAlias(tabletStatus.Tablet.Alias)
			matched, err := matchTabletByAlias(filter, tabletAlias)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}

			qr, err := tabletStatus.Conn.CommonQuery(context.Background(), "TabletsPlans", nil)
			if err != nil {
				return nil, err
			}

			rows = append(rows, qr.Rows...)
		}
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("tablet_alias", "query_template", "plan_type", "tables", "query_count", "accumulated_time", "accumulated_mysql_time", "rows_affected", "rows_returned", "error_count"),
		Rows:   rows,
	}, nil
}

func (e *Executor) showWorkload(_ *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	rows := [][]sqltypes.Value{}
	status := e.scatterConn.GetGatewayCacheStatus()
	for _, s := range status {
		rows = append(rows, sqltypes.BuildVarCharRow(
			s.Name,
			s.TabletType.String(),
			fmt.Sprintf("%v", s.QueryCount),
			fmt.Sprintf("%v", s.QPS),
			fmt.Sprintf("%v", s.AvgLatency),
			fmt.Sprintf("%v", s.QueryError),
		))
	}
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Tablet Alias", "Type", "TotalQueries(60s)", "Qps", "AvgLatency(ms)", "QueryError"),
		Rows:   rows,
	}, nil
}

func (e *Executor) showVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	rows := [][]sqltypes.Value{}

	status := e.scatterConn.GetHealthCheckCacheStatus()

	for _, s := range status {
		for _, ts := range s.TabletsStats {
			// We only want to show REPLICA and RDONLY tablets
			if ts.Tablet.Type != topodatapb.TabletType_REPLICA && ts.Tablet.Type != topodatapb.TabletType_RDONLY {
				continue
			}

			// Allow people to filter by Keyspace and Shard using a LIKE clause
			if filter != nil {
				ksFilterRegex := sqlparser.LikeToRegexp(filter.Like)
				keyspaceShardStr := fmt.Sprintf("%s/%s", ts.Tablet.Keyspace, ts.Tablet.Shard)
				if !ksFilterRegex.MatchString(keyspaceShardStr) {
					continue
				}
			}

			tabletHostPort := ts.GetTabletHostPort()
			throttlerStatus, err := getTabletThrottlerStatus(tabletHostPort)
			if err != nil {
				log.Warningf("Could not get throttler status from %s: %v", tabletHostPort, err)
			}

			replSourceHost := ""
			replSourcePort := int64(0)
			replIOThreadHealth := ""
			replSQLThreadHealth := ""
			replLastError := ""
			replLag := int64(-1)
			sql := "show slave status"
			results, err := e.txConn.tabletGateway.Execute(ctx, ts.Target, sql, nil, 0, 0, nil)
			if err != nil || results == nil {
				log.Warningf("Could not get replication status from %s: %v", tabletHostPort, err)
			} else if row := results.Named().Row(); row != nil {
				replSourceHost = row["Master_Host"].ToString()
				replSourcePort, _ = row["Master_Port"].ToInt64()
				replIOThreadHealth = row["Slave_IO_Running"].ToString()
				replSQLThreadHealth = row["Slave_SQL_Running"].ToString()
				replLastError = row["Last_Error"].ToString()
				if ts.Stats != nil {
					replLag = int64(ts.Stats.ReplicationLagSeconds)
				}
			}
			replicationHealth := fmt.Sprintf("{\"EventStreamRunning\":\"%s\",\"EventApplierRunning\":\"%s\",\"LastError\":\"%s\"}", replIOThreadHealth, replSQLThreadHealth, replLastError)

			rows = append(rows, sqltypes.BuildVarCharRow(
				s.Target.Keyspace,
				s.Target.Shard,
				ts.Target.TabletType.String(),
				topoproto.TabletAliasString(ts.Tablet.Alias),
				ts.Tablet.Hostname,
				fmt.Sprintf("%s:%d", replSourceHost, replSourcePort),
				replicationHealth,
				fmt.Sprintf("%d", replLag),
				throttlerStatus,
			))
		}
	}
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("Keyspace", "Shard", "TabletType", "Alias", "Hostname", "ReplicationSource", "ReplicationHealth", "ReplicationLag", "ThrottlerStatus"),
		Rows:   rows,
	}, nil
}

// MessageStream is part of the vtgate service API. This is a V2 level API that's sent
// to the Resolver.
func (e *Executor) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	err := e.resolver.MessageStream(
		ctx,
		keyspace,
		shard,
		keyRange,
		name,
		callback,
	)
	return formatError(err)
}

// VSchema returns the VSchema.
func (e *Executor) VSchema() *vindexes.VSchema {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.vschema
}

func (e *Executor) VSchemaAddKeyspaceIfNotExists(name string, KeyspaceSchema *vindexes.KeyspaceSchema) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.vschema.Keyspaces[name]; !ok {
		e.vschema.Keyspaces[name] = KeyspaceSchema
	}
}

func (e *Executor) VSchemaDeleteKeyspace(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.vschema.Keyspaces, name)
}

// SaveVSchema updates the vschema and stats
func (e *Executor) SaveVSchema(vschema *vindexes.VSchema, stats *VSchemaStats) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if vschema != nil {
		e.vschema = vschema
	}
	e.vschemaStats = stats
	e.plans.Clear()

	if vschemaCounters != nil {
		vschemaCounters.Add("Reload", 1)
	}

}

// ParseDestinationTarget parses destination target string and sets default keyspace if possible.
func (e *Executor) ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.Destination, error) {
	destKeyspace, destTabletType, dest, err := topoproto.ParseDestination(targetString, defaultTabletType)
	return destKeyspace, destTabletType, dest, err
}

type iQueryOption interface {
	cachePlan() bool
	getSelectLimit() int
}

// getPlan computes the plan for the given query. If one is in
// the cache, it reuses it.
func (e *Executor) getPlan(ctx context.Context, stmt sqlparser.Statement, reserved sqlparser.BindVars, vcursor *vcursorImpl, sql string, comments sqlparser.MarginComments, bindVars map[string]*querypb.BindVariable, qo iQueryOption, logStats *logstats.LogStats) (*engine.Plan, sqlparser.Statement, error) {
	if e.VSchema() == nil {
		return nil, nil, errors.New("vschema not initialized")
	}

	query := sql
	statement := stmt
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	bindVarNeeds := &sqlparser.BindVarNeeds{}
	if !sqlparser.IgnoreMaxPayloadSizeDirective(statement) && !isValidPayloadSize(query) {
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.NetPacketTooLarge, "query payload size above threshold")
	}
	ignoreMaxMemoryRows := sqlparser.IgnoreMaxMaxMemoryRowsDirective(stmt)
	vcursor.SetIgnoreMaxMemoryRows(ignoreMaxMemoryRows)
	consolidator := sqlparser.Consolidator(stmt)
	vcursor.SetConsolidator(consolidator)

	setVarComment, err := prepareSetVarComment(vcursor, stmt)
	if err != nil {
		return nil, nil, err
	}

	//rewrite TableName
	if vcursor.safeSession.GetOptions() != nil {
		vcursor.safeSession.GetOptions().IsSkipUse = false
		isRewrite := vcursor.Session().GetRewriteTableNameWithDbNamePrefix()
		if vcursor.keyspace != "" && isRewrite {
			var isSkipUse bool
			stmt, isSkipUse, err = sqlparser.RewriteTableName(stmt, vcursor.keyspace)
			if err != nil {
				return nil, nil, err
			}
			if isSkipUse {
				vcursor.safeSession.GetOptions().IsSkipUse = isSkipUse
				query = sqlparser.String(statement)
			}
		}
	}

	// Normalize if possible and retry.
	if e.canNormalizeStatement(stmt, qo, setVarComment) {
		parameterize := e.normalize // the public flag is called normalize
		result, err := sqlparser.PrepareAST(
			stmt,
			reservedVars,
			bindVars,
			parameterize,
			vcursor.keyspace,
			qo.getSelectLimit(),
			setVarComment,
			vcursor.safeSession.SystemVariables,
			vcursor,
		)
		if err != nil {
			return nil, nil, err
		}
		statement = result.AST
		bindVarNeeds = result.BindVarNeeds
		query = sqlparser.String(statement)
	}

	logStats.SQL = comments.Leading + query + comments.Trailing
	logStats.BindVariables = sqltypes.CopyBindVariables(bindVars)

	return e.cacheAndBuildStatement(ctx, vcursor, query, statement, qo, logStats, stmt, reservedVars, bindVarNeeds)
}

func getPlanKey(ctx context.Context, vcursor *vcursorImpl, query string) string {
	planHash := sha256.New()
	a := vcursor.planPrefixKey(ctx)
	_, _ = planHash.Write([]byte(a))
	_, _ = planHash.Write([]byte{':'})
	_, _ = planHash.Write(hack.StringBytes(query))
	planKey := hex.EncodeToString(planHash.Sum(nil))
	return planKey
}

func (e *Executor) cacheAndBuildStatement(ctx context.Context, vcursor *vcursorImpl, query string, statement sqlparser.Statement, qo iQueryOption, logStats *logstats.LogStats, stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, bindVarNeeds *sqlparser.BindVarNeeds) (*engine.Plan, sqlparser.Statement, error) {
	planKey := getPlanKey(ctx, vcursor, query)

	if sqlparser.CachePlan(statement) && qo.cachePlan() {
		if plan, ok := e.plans.Get(planKey); ok {
			logStats.CachedPlan = true
			return plan.(*engine.Plan), stmt, nil
		}
	}

	plan, err := planbuilder.BuildFromStmt(query, statement, reservedVars, vcursor, bindVarNeeds, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, nil, err
	}

	plan.Warnings = vcursor.warnings
	vcursor.warnings = nil

	err = e.checkThatPlanIsValid(stmt, plan)
	// Only cache the plan if it is valid (i.e. does not scatter)
	if err == nil && qo.cachePlan() && sqlparser.CachePlan(statement) {
		e.plans.Set(planKey, plan)
	}
	return plan, stmt, err
}

func (e *Executor) canNormalizeStatement(stmt sqlparser.Statement, qo iQueryOption, setVarComment string) bool {
	return (e.normalize && sqlparser.CanNormalize(stmt)) ||
		sqlparser.MustRewriteAST(stmt, qo.getSelectLimit() > 0) || setVarComment != ""
}

func prepareSetVarComment(vcursor *vcursorImpl, stmt sqlparser.Statement) (string, error) {
	if vcursor == nil || vcursor.Session().InReservedConn() {
		return "", nil
	}

	if !vcursor.Session().HasSystemVariables() {
		return "", nil
	}

	switch stmt.(type) {
	// If the statement is a transaction statement or a set no reserved connection / SET_VAR is needed
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint,
		*sqlparser.SRollback, *sqlparser.Release, *sqlparser.Set, *sqlparser.Show:
		return "", nil
	case sqlparser.SupportOptimizerHint:
		break
	default:
		vcursor.NeedsReservedConn()
		return "", nil
	}

	var res strings.Builder
	vcursor.Session().GetSystemVariables(func(k, v string) {
		res.WriteString(fmt.Sprintf("SET_VAR(%s = %s) ", k, v))
	})
	return strings.TrimSpace(res.String()), nil
}

func (e *Executor) debugGetPlan(planKey string) (*engine.Plan, bool) {
	planHash := sha256.Sum256([]byte(planKey))
	planHex := hex.EncodeToString(planHash[:])
	if plan, ok := e.plans.Get(planHex); ok {
		return plan.(*engine.Plan), true
	}
	return nil, false
}

type cacheItem struct {
	Key   string
	Value *engine.Plan
}

func (e *Executor) debugCacheEntries() (items []cacheItem) {
	e.plans.ForEach(func(value any) bool {
		plan := value.(*engine.Plan)
		items = append(items, cacheItem{
			Key:   plan.Original,
			Value: plan,
		})
		return true
	})
	return
}

// ServeHTTP shows the current plans in the query cache.
func (e *Executor) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}

	switch request.URL.Path {
	case pathQueryPlans:
		returnAsJSON(response, e.debugCacheEntries())
	case pathVSchema:
		returnAsJSON(response, e.VSchema())
	case pathScatterStats:
		e.WriteScatterStats(response)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func returnAsJSON(response http.ResponseWriter, stuff any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	buf, err := json.MarshalIndent(stuff, "", " ")
	if err != nil {
		_, _ = response.Write([]byte(err.Error()))
		return
	}
	ebuf := bytes.NewBuffer(nil)
	json.HTMLEscape(ebuf, buf)
	_, _ = response.Write(ebuf.Bytes())
}

// Plans returns the LRU plan cache
func (e *Executor) Plans() cache.Cache {
	return e.plans
}

func (e *Executor) updateQueryCounts(planType, keyspace, tableName string, shardQueries int64) {
	queriesProcessed.Add(planType, 1)
	queriesRouted.Add(planType, shardQueries)
	if tableName != "" {
		queriesProcessedByTable.Add([]string{planType, keyspace, tableName}, 1)
		queriesRoutedByTable.Add([]string{planType, keyspace, tableName}, shardQueries)
	}
}

// VSchemaStats returns the loaded vschema stats.
func (e *Executor) VSchemaStats() *VSchemaStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.vschemaStats == nil {
		return &VSchemaStats{
			Error: "No VSchema loaded yet.",
		}
	}
	return e.vschemaStats
}

// isValidPayloadSize validates whether a query payload is above the
// configured MaxPayloadSize threshold. The WarnPayloadSizeExceeded will increment
// if the payload size exceeds the warnPayloadSize.

func isValidPayloadSize(query string) bool {
	payloadSize := len(query)
	if maxPayloadSize > 0 && payloadSize > maxPayloadSize {
		return false
	}
	if warnPayloadSize > 0 && payloadSize > warnPayloadSize {
		warnings.Add("WarnPayloadSizeExceeded", 1)
	}
	return true
}

// Prepare executes a prepare statements.
func (e *Executor) Prepare(ctx context.Context, method string, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable) (fld []*querypb.Field, err error) {
	logStats := logstats.NewLogStats(ctx, method, sql, safeSession.GetSessionUUID(), bindVars)
	fld, err = e.prepare(ctx, safeSession, sql, bindVars, logStats)
	logStats.Error = err

	// The mysql plugin runs an implicit rollback whenever a connection closes.
	// To avoid spamming the log with no-op rollback records, ignore it if
	// it was a no-op record (i.e. didn't issue any queries)
	if !(logStats.StmtType == "ROLLBACK" && logStats.ShardQueries == 0) {
		logStats.SaveEndTime()
		QueryLogger.Send(logStats)
	}
	return fld, err
}

func (e *Executor) prepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) ([]*querypb.Field, error) {
	// Start an implicit transaction if necessary.
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession, nil); err != nil {
			return nil, err
		}
	}

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	stmtType := sqlparser.Preview(sql)
	logStats.StmtType = stmtType.String()

	// Mysql warnings are scoped to the current session, but are
	// cleared when a "non-diagnostic statement" is executed:
	// https://dev.mysql.com/doc/refman/8.0/en/show-warnings.html
	//
	// To emulate this behavior, clear warnings from the session
	// for all statements _except_ SHOW, so that SHOW WARNINGS
	// can actually return them.
	if stmtType != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	switch stmtType {
	case sqlparser.StmtSelect, sqlparser.StmtShow:
		return e.handlePrepare(ctx, safeSession, sql, bindVars, logStats)
	case sqlparser.StmtDDL, sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtRollback, sqlparser.StmtSet, sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete,
		sqlparser.StmtUse, sqlparser.StmtOther, sqlparser.StmtComment, sqlparser.StmtExplain, sqlparser.StmtFlush:
		return nil, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized prepare statement: %s", sql)
}

func (e *Executor) handlePrepare(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *logstats.LogStats) ([]*querypb.Field, error) {
	// V3 mode.
	query, comments := sqlparser.SplitMarginComments(sql)
	mystmt, reserved, err := sqlparser.Parse2(query)
	if err != nil {
		return nil, err
	}

	vcursor, _ := newVCursorImpl(safeSession, query, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, e.warnShardedOnly, e.pv)

	err = ResolveTabletType(safeSession, vcursor, mystmt, sql)
	if err != nil {
		return nil, err
	}

	plan, _, err := e.getPlan(ctx, mystmt, reserved, vcursor, query, comments, bindVars, safeSession, logStats)

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	if err != nil {
		logStats.Error = err
		return nil, err
	}

	err = e.addNeededBindVars(ctx, plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return nil, err
	}

	qr, err := plan.Instructions.GetFields(ctx, vcursor, bindVars)
	logStats.ExecuteTime = time.Since(execStart)
	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1 // nolint
		return nil, err
	}
	logStats.RowsAffected = qr.RowsAffected

	plan.AddStats(1, time.Since(logStats.StartTime), logStats.ShardQueries, qr.RowsAffected, uint64(len(qr.Rows)), errCount)

	return qr.Fields, err
}

// ExecuteMultiShard implements the IExecutor interface
func (e *Executor) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool) (qr *sqltypes.Result, errs []error) {
	return e.scatterConn.ExecuteMultiShard(ctx, primitive, rss, queries, session, autocommit, ignoreMaxMemoryRows)
}

// StreamExecuteMulti implements the IExecutor interface
func (e *Executor) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	return e.scatterConn.StreamExecuteMulti(ctx, primitive, query, rss, vars, session, autocommit, callback)
}

// ExecuteLock implements the IExecutor interface
func (e *Executor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	return e.scatterConn.ExecuteLock(ctx, rs, query, session, lockFuncType)
}

// ExecuteMessageStream implements the IExecutor interface
func (e *Executor) ExecuteMessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(reply *sqltypes.Result) error) error {
	return e.scatterConn.MessageStream(ctx, rss, tableName, callback)
}

// ExecuteVStream implements the IExecutor interface
func (e *Executor) ExecuteVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	return e.startVStream(ctx, rss, filter, gtid, callback)
}

func (e *Executor) startVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	var shardGtids []*binlogdatapb.ShardGtid
	for _, rs := range rss {
		shardGtid := &binlogdatapb.ShardGtid{
			Keyspace: rs.Target.Keyspace,
			Shard:    rs.Target.Shard,
			Gtid:     gtid,
		}
		shardGtids = append(shardGtids, shardGtid)
	}
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: shardGtids,
	}
	ts, err := e.serv.GetTopoServer()
	if err != nil {
		return err
	}

	vsm := newVStreamManager(e.resolver.resolver, e.serv, e.cell)
	vs := &vstream{
		vgtid:              vgtid,
		tabletType:         topodatapb.TabletType_PRIMARY,
		filter:             filter,
		send:               callback,
		resolver:           e.resolver.resolver,
		journaler:          make(map[int64]*journalEvent),
		skewTimeoutSeconds: maxSkewTimeoutSeconds,
		timestamps:         make(map[string]int64),
		vsm:                vsm,
		eventCh:            make(chan []*binlogdatapb.VEvent),
		ts:                 ts,
		copyCompletedShard: make(map[string]struct{}),
	}
	_ = vs.stream(ctx)
	return nil
}

func (e *Executor) SetFailPoint(command string, key string, value string) error {
	ctx := context.Background()
	var wg sync.WaitGroup
	healthyTablets := e.scatterConn.gateway.hc.GetAllHealthyTabletStats()
	errCh := make(chan error, len(healthyTablets))
	for _, tablet := range healthyTablets {
		wg.Add(1)
		go func(th *discovery.TabletHealth) {
			defer wg.Done()
			if err := th.Conn.SetFailPoint(ctx, command, key, value); err != nil {
				errCh <- err
			}
		}(tablet)
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

func findHealthyPrimaryTablet(hc discovery.HealthCheck) (*discovery.TabletHealth, error) {
	healthyTablets := hc.GetAllHealthyTabletStats()
	for _, tablet := range healthyTablets {
		if tablet.Tablet.Type == topodatapb.TabletType_PRIMARY {
			return tablet, nil
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no primary tablet found")
}

func (e *Executor) SubmitDMLJob(command, sql, uuid, tableSchema, timePeriodStart, timePeriodEnd, timePeriodTimeZone string, timeGapInMs, batchSize int64, postponeLaunch bool, failPolicy, throttleDuration, throttleRatio string) (*sqltypes.Result, error) {
	ctx := context.Background()
	th, err := findHealthyPrimaryTablet(e.scatterConn.gateway.hc)
	if err != nil {
		return nil, err
	}

	return th.Conn.SubmitDMLJob(ctx, command, sql, uuid, tableSchema, timePeriodStart, timePeriodEnd, timePeriodTimeZone, timeGapInMs, batchSize, postponeLaunch, failPolicy, throttleDuration, throttleRatio)
}

func (e *Executor) ShowDMLJob(uuid string, showDetails bool) (*sqltypes.Result, error) {
	ctx := context.Background()
	th, err := findHealthyPrimaryTablet(e.scatterConn.gateway.hc)
	if err != nil {
		return nil, err
	}

	return th.Conn.ShowDMLJob(ctx, uuid, showDetails)
}

func (e *Executor) HandleWescaleFilterRequest(sql string) (*sqltypes.Result, error) {
	ctx := context.Background()
	args := make(map[string]any)
	args["sql"] = sql
	args["isPrimary"] = true
	var rst *sqltypes.Result
	var rstErr error
	var primaryAlias *topodatapb.TabletAlias

	th, err := findHealthyPrimaryTablet(e.scatterConn.gateway.hc)
	if err != nil {
		return nil, err
	}
	rst, rstErr = th.Conn.CommonQuery(ctx, "HandleWescaleFilterRequest", args)
	if rstErr != nil {
		return nil, rstErr
	}

	// after sending request to primary successfully, notify others (there may be two primary tablets in wescale at the same time, so we compare the tablet alias)
	args["isPrimary"] = false
	for _, tablet := range e.scatterConn.gateway.hc.GetAllHealthyTabletStats() {
		if tablet.Tablet.Alias != primaryAlias {
			_, rstErr = tablet.Conn.CommonQuery(ctx, "HandleWescaleFilterRequest", args)
			if rstErr != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "err when sending wescale filter request to replica and rdonly: %v", rstErr)
			}
		}
	}

	return rst, nil
}

func (e *Executor) HandleWescaleCDCRequest(stmt sqlparser.Statement) (*sqltypes.Result, error) {
	th, err := findHealthyPrimaryTablet(e.scatterConn.gateway.hc)
	if err != nil {
		return nil, err
	}

	target := &querypb.Target{
		Keyspace:   global.DefaultKeyspace,
		Shard:      global.DefaultShard,
		TabletType: topodatapb.TabletType_PRIMARY,
	}

	query := ""
	switch s := stmt.(type) {
	case *sqlparser.CreateWescaleCDC:
		query, err = generateCreateWescaleCDCQuery(s)
	case *sqlparser.AlterWescaleCDC:
		query, err = generateAlterWescaleCDCQuery(s)
	case *sqlparser.DropWescaleCDC:
		query, err = generateDropWescaleCDCQuery(s)
	case *sqlparser.ShowWescaleCDC:
		if s.ShowCreate {
			return generateShowCreateCDCResult(th, target, s)
		}
		query, err = generateShowWescaleCDCQuery(s)
	}

	if err != nil {
		return nil, err
	}

	return th.Conn.ExecuteInternal(context.Background(), target, query, nil, 0, 0, nil)
}

// WescaleCDCEmptyValue If user didn't set the field value of CDC task in sql cmd,
// the default filed value will be set to rules.UnsetValueOfStmt in sql.y.
// If we use "" as default value, we won't recognize the situation
// that user alter the value to "" during alter cmd.
const (
	WescaleCDCEmptyValue         = rules.UnsetValueOfStmt
	WescaleCDCDescDefaultValue   = ""
	WescaleCDCEnableDefaultValue = "false"
	WescaleCDCEnvDefaultValue    = ""
)

func generateCreateWescaleCDCQuery(cdc *sqlparser.CreateWescaleCDC) (string, error) {
	// check validity of fields
	if cdc.Name == WescaleCDCEmptyValue {
		return "", fmt.Errorf("cdc name is required")
	}
	if cdc.WasmBinaryName == WescaleCDCEmptyValue {
		return "", fmt.Errorf("wasm binary name is required")
	}

	if cdc.Enable == WescaleCDCEmptyValue {
		cdc.Enable = WescaleCDCEnableDefaultValue
	}
	enable, err := strconv.ParseBool(cdc.Enable)
	if err != nil {
		return "", fmt.Errorf("enable field can't be parse as bool type, %v", err)
	}
	enableVal := 0
	if enable {
		enableVal = 1
	}

	// set default value for rest fields
	if cdc.Description == WescaleCDCEmptyValue {
		cdc.Description = WescaleCDCDescDefaultValue
	}
	if cdc.Env == WescaleCDCEmptyValue {
		cdc.Env = WescaleCDCEnvDefaultValue
	}

	templatePrefix := "insert "
	if cdc.IfNotExists {
		templatePrefix += "ignore "
	}
	template := templatePrefix + "into mysql.cdc_consumer (name, description, enable, wasm_binary_name, env, last_gtid, last_pk) values ('%s', '%s', %d, '%s', '%s', null, null);"

	return fmt.Sprintf(template, cdc.Name, cdc.Description, enableVal, cdc.WasmBinaryName, cdc.Env), nil
}

func generateAlterWescaleCDCQuery(cdc *sqlparser.AlterWescaleCDC) (string, error) {
	query := "update mysql.cdc_consumer set "
	var updates []string

	if cdc.Name != WescaleCDCEmptyValue {
		updates = append(updates, fmt.Sprintf("name = '%s'", cdc.Name))
	}

	if cdc.Description != WescaleCDCEmptyValue {
		updates = append(updates, fmt.Sprintf("description = '%s'", cdc.Description))
	}

	if cdc.Enable != WescaleCDCEmptyValue {
		enable, err := strconv.ParseBool(cdc.Enable)
		if err != nil {
			return "", fmt.Errorf("enable field can't be parse as bool type, %v", err)
		}
		enableVal := 0
		if enable {
			enableVal = 1
		}
		updates = append(updates, fmt.Sprintf("enable = %d", enableVal))
	}

	if cdc.WasmBinaryName != WescaleCDCEmptyValue {
		updates = append(updates, fmt.Sprintf("wasm_binary_name = '%s'", cdc.WasmBinaryName))
	}

	if cdc.Env != WescaleCDCEmptyValue {
		updates = append(updates, fmt.Sprintf("env = '%s'", cdc.Env))
	}

	if len(updates) == 0 {
		return "", fmt.Errorf("no fields to update")
	}

	query += strings.Join(updates, ", ")
	query += fmt.Sprintf(" where name = '%s';", cdc.OriginName)

	return query, nil
}

func generateDropWescaleCDCQuery(cdc *sqlparser.DropWescaleCDC) (string, error) {
	if cdc.Name == "" {
		return "", fmt.Errorf("cdc name is required")
	}
	return fmt.Sprintf("delete from mysql.cdc_consumer where name = '%s';", cdc.Name), nil
}

func generateShowWescaleCDCQuery(cdc *sqlparser.ShowWescaleCDC) (string, error) {
	selectAll := "select * from mysql.cdc_consumer"

	if cdc.Name == "" {
		return selectAll, nil
	}
	return selectAll + fmt.Sprintf(" where name = '%s';", cdc.Name), nil
}

func generateShowCreateCDCResult(th *discovery.TabletHealth, target *querypb.Target, showCreateCDC *sqlparser.ShowWescaleCDC) (*sqltypes.Result, error) {
	if !showCreateCDC.ShowCreate || showCreateCDC.Name == "" {
		return nil, fmt.Errorf("ShowCreate or Name is not set in generateShowCreateCDCResult")
	}

	query := fmt.Sprintf("select * FROM mysql.cdc_consumer where `name` = '%s'", showCreateCDC.Name)
	qr, err := th.Conn.ExecuteInternal(context.Background(), target, query, nil, 0, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("err when select cdc info in generateShowCreateCDCResult: %v", err)
	}
	if len(qr.Named().Rows) != 1 {
		return nil, fmt.Errorf("in generateShowCreateCDCResult: the expected cdc num is 1 but got %d", len(qr.Named().Rows))
	}

	// transfer query result to create cdc stmt
	cdcStmt := &sqlparser.CreateWescaleCDC{}
	row := qr.Named().Rows[0]
	cdcStmt.Name = row.AsString("name", "")
	cdcStmt.Description = row.AsString("description", "")
	cdcStmt.WasmBinaryName = row.AsString("wasm_binary_name", "")
	cdcStmt.Env = row.AsString("env", "")
	enable := row.AsInt64("enable", 0)
	if enable == 1 {
		cdcStmt.Enable = "true"
	} else {
		cdcStmt.Enable = "false"
	}

	// construct create cdc query result
	rows := [][]sqltypes.Value{}
	rows = append(rows, sqltypes.BuildVarCharRow(
		cdcStmt.Name,
		"\n"+sqlparser.String(cdcStmt),
	))
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("CDC", "Create CDC"),
		Rows:   rows,
	}, nil
}

func (e *Executor) checkThatPlanIsValid(stmt sqlparser.Statement, plan *engine.Plan) error {
	if e.allowScatter || plan.Instructions == nil || sqlparser.AllowScatterDirective(stmt) {
		return nil
	}
	// we go over all the primitives in the plan, searching for a route that is of SelectScatter opcode
	badPrimitive := engine.Find(func(node engine.Primitive) bool {
		router, ok := node.(*engine.Route)
		if !ok {
			return false
		}
		return router.Opcode == engine.Scatter
	}, plan.Instructions)

	if badPrimitive == nil {
		return nil
	}

	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "plan includes scatter, which is disallowed using the `no_scatter` command line argument")
}

func getTabletThrottlerStatus(tabletHostPort string) (string, error) {
	client := http.Client{
		Timeout: 100 * time.Millisecond,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/throttler/check?app=vtgate", tabletHostPort))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var elements struct {
		StatusCode int
		Value      float64
		Threshold  float64
		Message    string
	}
	err = json.Unmarshal(body, &elements)
	if err != nil {
		return "", err
	}

	httpStatusStr := http.StatusText(elements.StatusCode)

	load := float64(0)
	if elements.Threshold > 0 {
		load = float64((elements.Value / elements.Threshold) * 100)
	}

	status := fmt.Sprintf("{\"state\":\"%s\",\"load\":%.2f,\"message\":\"%s\"}", httpStatusStr, load, elements.Message)
	return status, nil
}
func (e *Executor) reloadExec(ctx context.Context, reloadType *sqlparser.ReloadType) error {
	tabletHealths := e.txConn.tabletGateway.hc.GetAllHealthyTabletStats()

	for _, tabletHealth := range tabletHealths {
		err := tabletHealth.Conn.ReloadExec(ctx, reloadType)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReleaseLock implements the IExecutor interface
func (e *Executor) ReleaseLock(ctx context.Context, session *SafeSession) error {
	return e.txConn.ReleaseLock(ctx, session)
}

func ParseTabletType(param string) (topodatapb.TabletType, error) {
	value, ok := topodatapb.TabletType_value[strings.ToUpper(param)]
	if !ok {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("unknown TabletType %v", param)
	}
	return topodatapb.TabletType(value), nil
}

func (e *Executor) FindHealthyPrimaryTablet() (*discovery.TabletHealth, error) {
	return findHealthyPrimaryTablet(e.scatterConn.gateway.hc)
}
