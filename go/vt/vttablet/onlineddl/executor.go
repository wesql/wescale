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

/*
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"

	"vitess.io/vitess/go/vt/failpointkey"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/sidecardb"

	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/vexec"
)

var (
	// ErrExecutorNotWritableTablet  is generated when executor is asked to run gh-ost on a read-only server
	ErrExecutorNotWritableTablet = errors.New("cannot run migration on non-writable tablet")
	// ErrExecutorMigrationAlreadyRunning is generated when an attempt is made to run an operation that conflicts with a running migration
	ErrExecutorMigrationAlreadyRunning = errors.New("cannot run migration since a migration is already running")
	// ErrMigrationNotFound is returned by readMigration when given UUI cannot be found
	ErrMigrationNotFound = errors.New("migration not found")
)

var vexecUpdateTemplates = []string{
	`update mysql.schema_migrations set migration_status='val1' where mysql_schema='val2'`,
	`update mysql.schema_migrations set migration_status='val1' where migration_uuid='val2' and mysql_schema='val3'`,
	`update mysql.schema_migrations set migration_status='val1' where migration_uuid='val2' and mysql_schema='val3' and shard='val4'`,
}

var vexecInsertTemplates = []string{
	`INSERT IGNORE INTO mysql.schema_migrations (
		migration_uuid,
		keyspace,
		shard,
		mysql_schema,
		mysql_table,
		migration_statement,
		strategy,
		options,
		ddl_action,
		requested_timestamp,
		migration_context,
		migration_status
	) VALUES (
		'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', FROM_UNIXTIME(0), 'vala', 'valb'
	)`,
}

var emptyResult = &sqltypes.Result{}
var acceptableDropTableIfExistsErrorCodes = []int{mysql.ERCantFindFile, mysql.ERNoSuchTable}

var (
	migrationCheckInterval  = 3 * time.Second
	retainOnlineDDLTables   = 24 * time.Hour
	maxConcurrentOnlineDDLs = 256
)

func init() {
	servenv.OnParseFor("vtcombo", registerOnlineDDLFlags)
	servenv.OnParseFor("vttablet", registerOnlineDDLFlags)
}

func registerOnlineDDLFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&migrationCheckInterval, "migration_check_interval", migrationCheckInterval, "Interval between migration checks")
	fs.DurationVar(&retainOnlineDDLTables, "retain_online_ddl_tables", retainOnlineDDLTables, "How long should vttablet keep an old migrated table before purging it")
	fs.IntVar(&maxConcurrentOnlineDDLs, "max_concurrent_online_ddl", maxConcurrentOnlineDDLs, "Maximum number of online DDL changes that may run concurrently")
}

var migrationNextCheckIntervals = []time.Duration{1 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}
var maxConstraintNameLength = 64

const (
	maxPasswordLength                        = 32 // MySQL's *replication* password may not exceed 32 characters
	staleMigrationMinutes                    = 180
	progressPctStarted               float64 = 0
	progressPctFull                  float64 = 100.0
	etaSecondsUnknown                        = -1
	etaSecondsNow                            = 0
	rowsCopiedUnknown                        = 0
	emptyHint                                = ""
	readyToCompleteHint                      = "ready_to_complete"
	databasePoolSize                         = 3
	vreplicationCutOverThreshold             = 5 * time.Second
	vreplicationTestSuiteWaitSeconds         = 5
)

var (
	migrationLogFileName     = "migration.log"
	migrationFailureFileName = "migration-failure.log"
	onlineDDLUser            = "vt-online-ddl-internal"
	onlineDDLGrant           = fmt.Sprintf("'%s'@'%s'", onlineDDLUser, "%")
	throttlerOnlineDDLApp    = "online-ddl"
	throttleCheckFlags       = &throttle.CheckFlags{}
)

type ConstraintType int

const (
	UnknownConstraintType ConstraintType = iota
	CheckConstraintType
	ForeignKeyConstraintType
)

var (
	constraintIndicatorMap = map[int]string{
		int(CheckConstraintType):      "chk",
		int(ForeignKeyConstraintType): "fk",
	}
)

func GetConstraintType(constraintInfo sqlparser.ConstraintInfo) ConstraintType {
	if _, ok := constraintInfo.(*sqlparser.CheckConstraintDefinition); ok {
		return CheckConstraintType
	}
	if _, ok := constraintInfo.(*sqlparser.ForeignKeyDefinition); ok {
		return ForeignKeyConstraintType
	}
	return UnknownConstraintType
}

type mysqlVariables struct {
	host           string
	port           int
	readOnly       bool
	version        string
	versionComment string
}

// Executor wraps and manages the execution of a gh-ost migration.
type Executor struct {
	env                   tabletenv.Env
	pool                  *connpool.Pool
	tabletTypeFunc        func() topodatapb.TabletType
	ts                    *topo.Server
	lagThrottler          *throttle.Throttler
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, bufferQueries bool)
	tabletAlias           *topodatapb.TabletAlias

	shard string

	initMutex      sync.Mutex
	migrationMutex sync.Mutex
	submitMutex    sync.Mutex // used when submitting migrations
	// ownedRunningMigrations lists UUIDs owned by this executor (consider this a map[string]bool)
	// A UUID listed in this map stands for a migration that is executing, and that this executor can control.
	// Migrations found to be running which are not listed in this map will either:
	// - be adopted by this executor (possible for vreplication migrations), or
	// - be terminated (example: pt-osc migration gone rogue, process still running even as the migration failed)
	// The Executor auto-reviews the map and cleans up migrations thought to be running which are not running.
	ownedRunningMigrations        sync.Map
	vreplicationLastError         map[string]*vterrors.LastError
	tickReentranceFlag            int64
	reviewedRunningMigrationsFlag bool

	ticks             *timer.Timer
	isOpen            int64
	schemaInitialized bool

	initVreplicationDDLOnce sync.Once
}

type cancellableMigration struct {
	uuid    string
	message string
}

func newCancellableMigration(uuid string, message string) *cancellableMigration {
	return &cancellableMigration{uuid: uuid, message: message}
}

// newGCTableRetainTime returns the time until which a new GC table is to be retained
func newGCTableRetainTime() time.Time {
	return time.Now().UTC().Add(retainOnlineDDLTables)
}

// NewExecutor creates a new gh-ost executor.
func NewExecutor(env tabletenv.Env, tabletAlias *topodatapb.TabletAlias, ts *topo.Server,
	lagThrottler *throttle.Throttler,
	tabletTypeFunc func() topodatapb.TabletType,
	toggleBufferTableFunc func(cancelCtx context.Context, tableName string, bufferQueries bool),
) *Executor {
	// sanitize flags
	if maxConcurrentOnlineDDLs < 1 {
		maxConcurrentOnlineDDLs = 1 // or else nothing will ever run
	}
	return &Executor{
		env:         env,
		tabletAlias: proto.Clone(tabletAlias).(*topodatapb.TabletAlias),

		pool: connpool.NewPool(env, "OnlineDDLExecutorPool", tabletenv.ConnPoolConfig{
			Size:               databasePoolSize,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		tabletTypeFunc:        tabletTypeFunc,
		ts:                    ts,
		lagThrottler:          lagThrottler,
		toggleBufferTableFunc: toggleBufferTableFunc,
		ticks:                 timer.NewTimer(migrationCheckInterval),
	}
}

// execQuery execute sql by using connect poll,so if targetString is not empty, it will add prefix `use database` first then execute sql.
func (e *Executor) execQuery(ctx context.Context, targetString, query string) (result *sqltypes.Result, err error) {
	defer e.env.LogError()
	var setting pools.Setting
	if targetString != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", targetString))
	}
	conn, err := e.pool.Get(ctx, &setting)
	if err != nil {
		return result, err
	}
	defer conn.Recycle()
	return conn.Exec(ctx, query, math.MaxInt32, true)
}

// TabletAliasString returns tablet alias as string (duh)
func (e *Executor) TabletAliasString() string {
	return topoproto.TabletAliasString(e.tabletAlias)
}

// InitDBConfig initializes keysapce
func (e *Executor) InitDBConfig(keyspace, shard, dbName string) {
	//e.keyspace = keyspace
	e.shard = shard
	//e.dbName = dbName
}

// Open opens database pool and initializes the schema
func (e *Executor) Open() error {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if atomic.LoadInt64(&e.isOpen) > 0 || !e.env.Config().EnableOnlineDDL {
		return nil
	}
	log.Infof("onlineDDL Executor Open()")

	e.reviewedRunningMigrationsFlag = false // will be set as "true" by reviewRunningMigrations()
	e.ownedRunningMigrations.Range(func(k, _ any) bool {
		e.ownedRunningMigrations.Delete(k)
		return true
	})
	e.vreplicationLastError = make(map[string]*vterrors.LastError)

	e.pool.Open(e.env.Config().DB.AppConnector(), e.env.Config().DB.DbaConnector(), e.env.Config().DB.AppDebugConnector())
	e.ticks.Start(e.onMigrationCheckTick)
	e.triggerNextCheckInterval()

	if _, err := sqlparser.QueryMatchesTemplates("select 1 from dual", vexecUpdateTemplates); err != nil {
		// this validates vexecUpdateTemplates
		return err
	}
	atomic.StoreInt64(&e.isOpen, 1)

	return nil
}

// Close frees resources
func (e *Executor) Close() {
	e.initMutex.Lock()
	defer e.initMutex.Unlock()
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return
	}
	log.Infof("onlineDDL Executor Close()")

	e.ticks.Stop()
	e.pool.Close()
	atomic.StoreInt64(&e.isOpen, 0)
}

// triggerNextCheckInterval the next tick sooner than normal
func (e *Executor) triggerNextCheckInterval() {
	for _, interval := range migrationNextCheckIntervals {
		e.ticks.TriggerAfter(interval)
	}
}

// matchesShards checks whether given comma delimited shard names include this tablet's shard. If the input param is empty then
// that implicitly means "true"
func (e *Executor) matchesShards(commaDelimitedShards string) bool {
	shards := textutil.SplitDelimitedList(commaDelimitedShards)
	if len(shards) == 0 {
		// Nothing explicitly defined, so implicitly all shards are allowed
		return true
	}
	for _, shard := range shards {
		if shard == e.shard {
			return true
		}
	}
	return false
}

// countOwnedRunningMigrations returns an estimate of current count of running migrations; this is
// normally an accurate number, but can be inexact because the exdcutor peridocially reviews
// e.ownedRunningMigrations and adds/removes migrations based on actual migration state.
func (e *Executor) countOwnedRunningMigrations() (count int) {
	e.ownedRunningMigrations.Range(func(_, val any) bool {
		if _, ok := val.(*schema.OnlineDDL); ok {
			count++
		}
		return true // continue iteration
	})
	return count
}

// allowConcurrentMigration checks if the given migration is allowed to run concurrently.
// First, the migration itself must declare --allow-concurrent. But then, there's also some
// restrictions on which migrations exactly are allowed such concurrency.
func (e *Executor) allowConcurrentMigration(onlineDDL *schema.OnlineDDL) (action sqlparser.DDLAction, allowConcurrent bool) {
	if !onlineDDL.StrategySetting().IsAllowConcurrent() {
		return action, false
	}

	var err error
	action, err = onlineDDL.GetAction()
	if err != nil {
		return action, false
	}
	switch action {
	case sqlparser.CreateDDLAction, sqlparser.DropDDLAction:
		// CREATE TABLE, DROP TABLE are allowed to run concurrently.
		return action, true
	case sqlparser.AlterDDLAction:
		// ALTER is only allowed concurrent execution if this is a Vitess migration
		strategy := onlineDDL.StrategySetting().Strategy
		return action, (strategy == schema.DDLStrategyOnline || strategy == schema.DDLStrategyVitess)
	case sqlparser.RevertDDLAction:
		// REVERT is allowed to run concurrently.
		// Reminder that REVERT is supported for CREATE, DROP and for 'vitess' ALTER, but never for
		// 'gh-ost' or 'pt-osc' ALTERs
		return action, true
	}
	return action, false
}

func (e *Executor) proposedMigrationConflictsWithRunningMigration(runningMigration, proposedMigration *schema.OnlineDDL) bool {
	if runningMigration.Table == proposedMigration.Table {
		// migrations operate on same table
		return true
	}
	_, isRunningMigrationAllowConcurrent := e.allowConcurrentMigration(runningMigration)
	proposedMigrationAction, isProposedMigrationAllowConcurrent := e.allowConcurrentMigration(proposedMigration)
	if !isRunningMigrationAllowConcurrent && !isProposedMigrationAllowConcurrent {
		// neither allowed concurrently
		return true
	}
	if proposedMigrationAction == sqlparser.AlterDDLAction {
		// A new ALTER migration conflicts with an existing migration if the existing migration is still not ready to complete.
		// Specifically, if the running migration is an ALTER, and is still busy with copying rows (copy_state), then
		// we consider the two to be conflicting. But, if the running migration is done copying rows, and is now only
		// applying binary logs, and is up-to-date, then we consider a new ALTER migration to be non-conflicting.
		return atomic.LoadInt64(&runningMigration.ReadyToComplete) == 0
	}
	return false
}

// isAnyConflictingMigrationRunning checks if there's any running migration that conflicts with the
// given migration, such that they can't both run concurrently.
func (e *Executor) isAnyConflictingMigrationRunning(onlineDDL *schema.OnlineDDL) (conflictFound bool, conflictingMigration *schema.OnlineDDL) {
	e.ownedRunningMigrations.Range(func(_, val any) bool {
		runningMigration, ok := val.(*schema.OnlineDDL)
		if !ok {
			return true // continue iteration
		}
		if e.proposedMigrationConflictsWithRunningMigration(runningMigration, onlineDDL) {
			conflictingMigration = runningMigration
			return false // stop iteration, no need to review other migrations
		}
		return true // continue iteration
	})
	return (conflictingMigration != nil), conflictingMigration
}

func (e *Executor) ptPidFileName(uuid string) string {
	return path.Join(os.TempDir(), fmt.Sprintf("pt-online-schema-change.%s.pid", uuid))
}

// readMySQLVariables contacts the backend MySQL server to read some of its configuration
func (e *Executor) readMySQLVariables(ctx context.Context) (variables *mysqlVariables, err error) {
	conn, err := e.pool.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, `select
			@@global.hostname as hostname,
			@@global.port as port,
			@@global.read_only as read_only,
			@@global.version AS version,
			@@global.version_comment AS version_comment
		from dual`, 1, true)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read MySQL variables: %v", err)
	}
	row := tm.Named().Row()
	if row == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL variables: %+v", tm.Rows)
	}
	variables = &mysqlVariables{}

	if e.env.Config().DB.Host != "" {
		variables.host = e.env.Config().DB.Host
	} else {
		variables.host = row["hostname"].ToString()
	}

	if e.env.Config().DB.Port != 0 {
		variables.port = e.env.Config().DB.Port
	} else if port, err := row.ToInt64("port"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.port %v: %v", tm, err)
	} else {
		variables.port = int(port)
	}
	if variables.readOnly, err = row.ToBool("read_only"); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse @@global.read_only %v: %v", tm, err)
	}

	variables.version = row["version"].ToString()
	variables.versionComment = row["version_comment"].ToString()

	return variables, nil
}

// tableExists checks if a given table exists.
func (e *Executor) tableExists(ctx context.Context, tableSchema, tableName string) (bool, error) {
	tableName = strings.ReplaceAll(tableName, `_`, `\_`)
	parsed := sqlparser.BuildParsedQuery(sqlShowTablesLike, tableName)
	rs, err := e.execQuery(ctx, tableSchema, parsed.Query)
	if err != nil {
		return false, err
	}
	row := rs.Named().Row()
	return (row != nil), nil
}

// showCreateTable returns the SHOW CREATE statement for a table or a view
func (e *Executor) showCreateTable(ctx context.Context, tableSchema, tableName string) (string, error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowCreateTable, tableSchema, tableName)
	rs, err := e.execQuery(ctx, tableSchema, parsed.Query)
	if err != nil {
		return "", err
	}
	if len(rs.Rows) == 0 {
		return "", nil
	}
	row := rs.Rows[0]
	return row[1].ToString(), nil
}

// executeDirectly runs a DDL query directly on the backend MySQL server
func (e *Executor) executeDirectly(ctx context.Context, onlineDDL *schema.OnlineDDL, acceptableMySQLErrorCodes ...int) (acceptableErrorCodeFound bool, err error) {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return false, err
	}
	defer conn.Close()

	restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
	defer restoreSQLModeFunc()
	if err != nil {
		return false, err
	}

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint)
	if onlineDDL.Schema != "" {
		_, err = conn.ExecuteFetch(fmt.Sprintf("use %s", onlineDDL.Schema), 0, false)
		if err != nil {
			return false, err
		}
	}
	_, err = conn.ExecuteFetch(onlineDDL.SQL, 0, false)

	if err != nil {
		// let's see if this error is actually acceptable
		if merr, ok := err.(*mysql.SQLError); ok {
			for _, acceptableCode := range acceptableMySQLErrorCodes {
				if merr.Num == acceptableCode {
					// we don't consider this to be an error.
					acceptableErrorCodeFound = true
					err = nil
					break
				}
			}
		}
	}
	if err != nil {
		return false, err
	}
	defer e.reloadSchema(ctx)
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)

	return acceptableErrorCodeFound, nil
}

// doesConnectionInfoMatch checks if theres a MySQL connection in PROCESSLIST whose Info matches given text
func (e *Executor) doesConnectionInfoMatch(ctx context.Context, connID int64, submatch string) (bool, error) {
	findProcessQuery, err := sqlparser.ParseAndBind(sqlFindProcess,
		sqltypes.Int64BindVariable(connID),
		sqltypes.StringBindVariable("%"+submatch+"%"),
	)
	if err != nil {
		return false, err
	}
	rs, err := e.execQuery(ctx, "information_schema", findProcessQuery)
	if err != nil {
		return false, err
	}
	return len(rs.Rows) == 1, nil
}

// tableParticipatesInForeignKeyRelationship checks if a given table is either a parent or a child in at least one foreign key constraint
func (e *Executor) tableParticipatesInForeignKeyRelationship(ctx context.Context, schema string, table string) (bool, error) {
	for _, fkQuery := range []string{selSelectCountFKParentConstraints, selSelectCountFKChildConstraints} {
		query, err := sqlparser.ParseAndBind(fkQuery,
			sqltypes.StringBindVariable(schema),
			sqltypes.StringBindVariable(table),
		)
		if err != nil {
			return false, err
		}
		r, err := e.execQuery(ctx, schema, query)
		if err != nil {
			return false, err
		}
		row := r.Named().Row()
		if row == nil {
			return false, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result from INFORMATION_SCHEMA.KEY_COLUMN_USAGE query: %s", query)
		}
		countFKConstraints := row.AsInt64("num_fk_constraints", 0)
		if countFKConstraints > 0 {
			return true, nil
		}
	}
	return false, nil
}

// validateTableForAlterAction checks whether a table is good to undergo a ALTER operation. It returns detailed error if not.
func (e *Executor) validateTableForAlterAction(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	if !onlineDDL.StrategySetting().IsAllowForeignKeysFlag() {
		// Validate table does not participate in foreign key relationship:
		participates, err := e.tableParticipatesInForeignKeyRelationship(ctx, onlineDDL.Schema, onlineDDL.Table)
		if err != nil {
			return vterrors.Wrapf(err, "error while attempting to validate whether table %s participates in FOREIGN KEY constraint", onlineDDL.Table)
		}
		if participates {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s participates in a FOREIGN KEY constraint and FOREIGN KEY constraints are not supported in Online DDL unless the *experimental and unsafe* --unsafe-allow-foreign-keys strategy flag is specified", onlineDDL.Table)
		}
	}
	return nil
}

// primaryPosition returns the MySQL/MariaDB position (typically GTID pos) on the tablet
func (e *Executor) primaryPosition(ctx context.Context) (pos mysql.Position, err error) {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return pos, err
	}
	defer conn.Close()

	pos, err = conn.PrimaryPosition()
	return pos, err
}

// terminateVReplMigration stops vreplication, then removes the mysql.vreplication entry for the given migration
func (e *Executor) terminateVReplMigration(ctx context.Context, tableSchma string, uuid string) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlStopVReplStream,
		sqltypes.StringBindVariable(tableSchma),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	// silently skip error; stopping the stream is just a graceful act; later deleting it is more important
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		log.Errorf("FAIL vreplicationExec: uuid=%s, query=%v, error=%v", uuid, query, err)
	}

	if err := e.deleteVReplicationEntry(ctx, tableSchma, uuid); err != nil {
		return err
	}
	return nil
}

// cutOverVReplMigration stops vreplication, then removes the mysql.vreplication entry for the given migration
func (e *Executor) cutOverVReplMigration(ctx context.Context, s *VReplStream) error {
	if err := e.incrementCutoverAttempts(ctx, s.workflow); err != nil {
		return err
	}

	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	// sanity checks:
	vreplTable, err := getVreplTable(ctx, s)
	if err != nil {
		return err
	}

	// get topology client & entities:
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	// information about source tablet
	onlineDDL, _, err := e.readMigration(ctx, s.workflow)
	if err != nil {
		return err
	}
	isVreplicationTestSuite := onlineDDL.StrategySetting().IsVreplicationTestSuite()
	e.updateMigrationStage(ctx, onlineDDL.UUID, "starting cut-over")

	var sentryTableName string

	waitForPos := func(s *VReplStream, pos mysql.Position) error {
		ctx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
		defer cancel()
		// Wait for target to reach the up-to-date pos
		if err := tmClient.VReplicationWaitForPos(ctx, tablet.Tablet, int(s.id), mysql.EncodePosition(pos)); err != nil {
			return err
		}
		// Target is now in sync with source!
		return nil
	}

	if !isVreplicationTestSuite {
		// A bit early on, we generate names for stowaway and temporary tables
		// We do this here because right now we're in a safe place where nothing happened yet. If there's an error now, bail out
		// and no harm done.
		// Later on, when traffic is blocked and tables renamed, that's a more dangerous place to be in; we want as little logic
		// in that place as possible.
		sentryTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
		if err != nil {
			return nil
		}

		// We create the sentry table before toggling writes, because this involves a WaitForPos, which takes some time. We
		// don't want to overload the buffering time with this excessive wait.

		if err := e.updateArtifacts(ctx, onlineDDL.UUID, sentryTableName); err != nil {
			return err
		}
		parsed := sqlparser.BuildParsedQuery(sqlCreateSentryTable, sentryTableName)
		if _, err := e.execQuery(ctx, onlineDDL.Schema, parsed.Query); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "sentry table created: %s", sentryTableName)

		postSentryPos, err := e.primaryPosition(ctx)
		if err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for post-sentry pos: %v", mysql.EncodePosition(postSentryPos))
		if err := waitForPos(s, postSentryPos); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "post-sentry pos reached")
	}

	var setting pools.Setting
	setting.SetWithoutDBName(false)
	setting.SetQuery(fmt.Sprintf("use %s", onlineDDL.Schema))

	lockConn, err := e.pool.Get(ctx, &setting)
	if err != nil {
		return err
	}
	defer lockConn.Recycle()
	defer lockConn.Exec(ctx, sqlUnlockTables, 1, false)

	renameConn, err := e.pool.Get(ctx, &setting)
	if err != nil {
		return err
	}
	defer renameConn.Recycle()
	defer renameConn.Kill("premature exit while renaming tables", 0)
	renameQuery := sqlparser.BuildParsedQuery(sqlSwapTables, onlineDDL.Table, sentryTableName, vreplTable, onlineDDL.Table, sentryTableName, vreplTable)

	waitForRenameProcess := func() error {
		// This function waits until it finds the RENAME TABLE... query running in MySQL's PROCESSLIST, or until timeout
		// The function assumes that one of the renamed tables is locked, thus causing the RENAME to block. If nothing
		// is locked, then the RENAME will be near-instantaneious and it's unlikely that the function will find it.
		renameWaitCtx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
		defer cancel()

		for {
			renameProcessFound, err := e.doesConnectionInfoMatch(renameWaitCtx, renameConn.ID(), "rename")
			if err != nil {
				return err
			}
			if renameProcessFound {
				return nil
			}
			select {
			case <-renameWaitCtx.Done():
				return vterrors.Errorf(vtrpcpb.Code_ABORTED, "timeout for rename query: %s", renameQuery.Query)
			case <-time.After(time.Second):
				// sleep
			}
		}
	}

	renameCompleteChan := make(chan error)

	bufferingCtx, bufferingContextCancel := context.WithCancel(ctx)
	defer bufferingContextCancel()
	// Preparation is complete. We proceed to cut-over.
	toggleBuffering := func(bufferQueries bool) error {
		log.Infof("toggling buffering: %t in migration %v", bufferQueries, onlineDDL.UUID)
		e.toggleBufferTableFunc(bufferingCtx, fmt.Sprintf("%s.%s", onlineDDL.Schema, onlineDDL.Table), bufferQueries)
		if !bufferQueries {
			// called after new table is in place.
			// unbuffer existing queries:
			bufferingContextCancel()
			// force re-read of tables
			if err := tmClient.RefreshState(ctx, tablet.Tablet); err != nil {
				return err
			}
		}
		log.Infof("toggled buffering: %t in migration %v", bufferQueries, onlineDDL.UUID)
		return nil
	}

	var reenableOnce sync.Once
	reenableWritesOnce := func() {
		reenableOnce.Do(func() {
			log.Infof("re-enabling writes in migration %v", onlineDDL.UUID)
			toggleBuffering(false)
			go log.Infof("cutOverVReplMigration %v: unbuffered queries", s.workflow)
		})
	}
	e.updateMigrationStage(ctx, onlineDDL.UUID, "buffering queries")
	// stop writes on source:
	err = toggleBuffering(true)
	defer reenableWritesOnce()
	if err != nil {
		return err
	}
	// Give a fraction of a second for a scenario where a query is in
	// query executor, it passed the ACLs and is _about to_ execute. This will be nicer to those queries:
	// they will be able to complete before the rename, rather than block briefly on the rename only to find
	// the table no longer exists.
	e.updateMigrationStage(ctx, onlineDDL.UUID, "graceful wait for buffering")
	time.Sleep(100 * time.Millisecond)

	if isVreplicationTestSuite {
		// The testing suite may inject queries internally from the server via a recurring EVENT.
		// Those queries are unaffected by query rules (ACLs) because they don't go through Vitess.
		// We therefore hard-rename the table into an agreed upon name, and we won't swap it with
		// the original table. We will actually make the table disappear, creating a void.
		testSuiteBeforeTableName := fmt.Sprintf("%s_before", onlineDDL.Table)
		parsed := sqlparser.BuildParsedQuery(sqlRenameTable, onlineDDL.Table, testSuiteBeforeTableName)
		if _, err := e.execQuery(ctx, onlineDDL.Schema, parsed.Query); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "test suite 'before' table renamed")
	} else {
		// real production

		e.updateMigrationStage(ctx, onlineDDL.UUID, "locking tables")
		lockCtx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
		defer cancel()
		lockTableQuery := sqlparser.BuildParsedQuery(sqlLockTwoTablesWrite, sentryTableName, onlineDDL.Table)
		if _, err := lockConn.Exec(lockCtx, lockTableQuery.Query, 1, false); err != nil {
			return err
		}

		e.updateMigrationStage(ctx, onlineDDL.UUID, "renaming tables")
		go func() {
			_, err := renameConn.Exec(ctx, renameQuery.Query, 1, false)
			renameCompleteChan <- err
		}()
		// the rename should block, because of the LOCK. Wait for it to show up.
		e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for RENAME to block")
		if err := waitForRenameProcess(); err != nil {
			return err
		}
		e.updateMigrationStage(ctx, onlineDDL.UUID, "RENAME found")
	}

	e.updateMigrationStage(ctx, onlineDDL.UUID, "reading post-lock pos")
	postWritesPos, err := e.primaryPosition(ctx)
	if err != nil {
		return err
	}

	// Right now: new queries are buffered, any existing query will have executed, and worst case scenario is
	// that some leftover query finds the table is not actually there anymore...
	// At any case, there's definitely no more writes to the table since it does not exist. We can
	// safely take the (GTID) pos now.
	_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", s.workflow)

	// Writes are now disabled on table. Read up-to-date vreplication info, specifically to get latest (and fixed) pos:
	s, err = e.readVReplStream(ctx, s.workflow, false)
	if err != nil {
		return err
	}

	e.updateMigrationStage(ctx, onlineDDL.UUID, "waiting for post-lock pos: %v", mysql.EncodePosition(postWritesPos))
	if err := waitForPos(s, postWritesPos); err != nil {
		e.updateMigrationStage(ctx, onlineDDL.UUID, "timeout while waiting for post-lock pos: %v", err)
		return err
	}
	go log.Infof("cutOverVReplMigration %v: done waiting for position %v", s.workflow, mysql.EncodePosition(postWritesPos))
	// Stop vreplication
	e.updateMigrationStage(ctx, onlineDDL.UUID, "stopping vreplication")
	failpoint.Inject(failpointkey.WaitJustBeforeStopVreplication.Name, func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(1 * time.Minute)
		}
	})
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, binlogplayer.StopVReplication(uint32(s.id), "stopped for online DDL cutover")); err != nil {
		return err
	}
	go log.Infof("cutOverVReplMigration %v: stopped vreplication", s.workflow)
	failpoint.Inject(failpointkey.WaitJustAfterStopVreplication.Name, func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(1 * time.Minute)
		}
	})

	// rename tables atomically (remember, writes on source tables are stopped)
	{
		if isVreplicationTestSuite {
			// this is used in Vitess endtoend testing suite
			testSuiteAfterTableName := fmt.Sprintf("%s_after", onlineDDL.Table)
			parsed := sqlparser.BuildParsedQuery(sqlRenameTable, vreplTable, testSuiteAfterTableName)
			if _, err := e.execQuery(ctx, onlineDDL.Schema, parsed.Query); err != nil {
				return err
			}
			e.updateMigrationStage(ctx, onlineDDL.UUID, "test suite 'after' table renamed")
		} else {
			e.updateMigrationStage(ctx, onlineDDL.UUID, "validating rename is still in place")
			if err := waitForRenameProcess(); err != nil {
				return err
			}

			// Normal (non-testing) alter table
			e.updateMigrationStage(ctx, onlineDDL.UUID, "dropping sentry table")

			{
				dropTableQuery := sqlparser.BuildParsedQuery(sqlDropTable, sentryTableName)
				lockCtx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
				defer cancel()
				if _, err := lockConn.Exec(lockCtx, dropTableQuery.Query, 1, false); err != nil {
					return err
				}
			}
			{
				lockCtx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
				defer cancel()
				e.updateMigrationStage(ctx, onlineDDL.UUID, "unlocking tables")
				if _, err := lockConn.Exec(lockCtx, sqlUnlockTables, 1, false); err != nil {
					return err
				}
			}
			{
				lockCtx, cancel := context.WithTimeout(ctx, vreplicationCutOverThreshold)
				defer cancel()
				e.updateMigrationStage(lockCtx, onlineDDL.UUID, "waiting for RENAME to complete")
				if err := <-renameCompleteChan; err != nil {
					return err
				}
			}
		}
	}
	e.updateMigrationStage(ctx, onlineDDL.UUID, "cut-over complete")
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)

	go func() {
		// Tables are swapped! Let's take the opportunity to ReloadSchema now
		// We do this in a goroutine because it might take time on a schema with thousands of tables, and we don't want to delay
		// the cut-over.
		// this means ReloadSchema is not in sync with the actual schema change. Users will still need to run tracker if they want to sync.
		// In the future, we will want to reload the single table, instead of reloading the schema.
		if err := e.reloadSchema(ctx); err != nil {
			vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Error on ReloadSchema while cutting over vreplication migration UUID: %+v", onlineDDL.UUID)
		}
	}()

	// Tables are now swapped! Migration is successful
	e.updateMigrationStage(ctx, onlineDDL.UUID, "re-enabling writes")
	reenableWritesOnce() // this function is also deferred, in case of early return; but now would be a good time to resume writes, before we publish the migration as "complete"
	go log.Infof("cutOverVReplMigration %v: marking as complete", s.workflow)
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, s.rowsCopied, emptyHint)
	return nil

	// deferred function will re-enable writes now
}

// initMigrationSQLMode sets sql_mode according to DDL strategy, and returns a function that
// restores sql_mode to original state
func (e *Executor) initMigrationSQLMode(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (deferFunc func(), err error) {
	deferFunc = func() {}
	if !onlineDDL.StrategySetting().IsAllowZeroInDateFlag() {
		// No need to change sql_mode.
		return deferFunc, nil
	}

	// Grab current sql_mode value
	rs, err := conn.ExecuteFetch(`select @@session.sql_mode as sql_mode`, 1, true)
	if err != nil {
		return deferFunc, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}
	sqlMode, err := rs.Named().Row().ToString("sql_mode")
	if err != nil {
		return deferFunc, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}
	// Pre-calculate restore function
	deferFunc = func() {
		restoreSQLModeQuery := fmt.Sprintf("set @@session.sql_mode='%s'", sqlMode)
		conn.ExecuteFetch(restoreSQLModeQuery, 0, false)
	}
	// Change sql_mode
	changeSQLModeQuery := fmt.Sprintf("set @@session.sql_mode=REPLACE(REPLACE('%s', 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')", sqlMode)
	if _, err := conn.ExecuteFetch(changeSQLModeQuery, 0, false); err != nil {
		return deferFunc, err
	}
	return deferFunc, nil
}

// newConstraintName generates a new, unique name for a constraint. Our problem is that a MySQL
// constraint's name is unique in the schema (!). And so as we duplicate the original table, we must
// create completely new names for all constraints.
// Moreover, we really want this name to be consistent across all shards. We therefore use a deterministic
// UUIDv5 (SHA) function over the migration UUID, table name, and constraint's _contents_.
// We _also_ include the original constraint name as prefix, as room allows
// for example, if the original constraint name is "check_1",
// we might generate "check_1_cps1okb4uafunfqusi2lp22u3".
// If we then again migrate a table whose constraint name is "check_1_cps1okb4uafunfqusi2lp22u3	" we
// get for example "check_1_19l09s37kbhj4axnzmi10e18k" (hash changes, and we still try to preserve original name)
//
// Furthermore, per bug report https://bugs.mysql.com/bug.php?id=107772, if the user doesn't provide a name for
// their CHECK constraint, then MySQL picks a name in this format <tablename>_chk_<number>.
// Example: sometable_chk_1
// Next, when MySQL is asked to RENAME TABLE and sees a constraint with this format, it attempts to rename
// the constraint with the new table's name. This is problematic for Vitess, because we often rename tables to
// very long names, such as _vt_HOLD_394f9e6dfc3d11eca0390a43f95f28a3_20220706091048.
// As we rename the constraint to e.g. `sometable_chk_1_cps1okb4uafunfqusi2lp22u3`, this makes MySQL want to
// call the new constraint something like _vt_HOLD_394f9e6dfc3d11eca0390a43f95f28a3_20220706091048_chk_1_cps1okb4uafunfqusi2lp22u3,
// which exceeds the 64 character limit for table names. Long story short, we also trim down <tablename> if the constraint seems
// to be auto-generated.
func (e *Executor) newConstraintName(onlineDDL *schema.OnlineDDL, constraintType ConstraintType, hashExists map[string]bool, seed string, oldName string) string {
	constraintIndicator := constraintIndicatorMap[int(constraintType)]
	oldName = schemadiff.ExtractConstraintOriginalName(oldName)
	autoGeneratedName := fmt.Sprintf("%s_%s_", onlineDDL.Table, constraintIndicator)
	if strings.HasPrefix(oldName, autoGeneratedName) {
		// strip out table name
		oldName = constraintIndicator + "_" + oldName[len(autoGeneratedName):]
	}

	hash := textutil.UUIDv5Base36(onlineDDL.UUID, onlineDDL.Table, seed)
	for i := 1; hashExists[hash]; i++ {
		hash = textutil.UUIDv5Base36(onlineDDL.UUID, onlineDDL.Table, seed, fmt.Sprintf("%d", i))
	}
	hashExists[hash] = true
	suffix := "_" + hash
	maxAllowedNameLength := maxConstraintNameLength - len(suffix)
	newName := oldName
	if newName == "" {
		newName = constraintIndicator // start with something that looks consistent with MySQL's naming
	}
	if len(newName) > maxAllowedNameLength {
		newName = newName[0:maxAllowedNameLength]
	}
	newName = newName + suffix
	return newName
}

// validateAndEditCreateTableStatement inspects the CreateTable AST and does the following:
// - extra validation (no FKs for now...)
// - generate new and unique names for all constraints (CHECK and FK; yes, why not handle FK names; even as we don't support FKs today, we may in the future)
func (e *Executor) validateAndEditCreateTableStatement(ctx context.Context, onlineDDL *schema.OnlineDDL, createTable *sqlparser.CreateTable) (constraintMap map[string]string, err error) {
	constraintMap = map[string]string{}
	hashExists := map[string]bool{}

	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ForeignKeyDefinition:
			if !onlineDDL.StrategySetting().IsAllowForeignKeysFlag() {
				return false, schema.ErrForeignKeyFound
			}
		case *sqlparser.ConstraintDefinition:
			oldName := node.Name.String()
			newName := e.newConstraintName(onlineDDL, GetConstraintType(node.Details), hashExists, sqlparser.CanonicalString(node.Details), oldName)
			node.Name = sqlparser.NewIdentifierCI(newName)
			constraintMap[oldName] = newName
		}
		return true, nil
	}
	if err := sqlparser.Walk(validateWalk, createTable); err != nil {
		return constraintMap, err
	}
	return constraintMap, nil
}

// validateAndEditAlterTableStatement inspects the AlterTable statement and:
// - modifies any CONSTRAINT name according to given name mapping
// - explode ADD FULLTEXT KEY into multiple statements
func (e *Executor) validateAndEditAlterTableStatement(ctx context.Context, onlineDDL *schema.OnlineDDL, alterTable *sqlparser.AlterTable, constraintMap map[string]string) (alters []*sqlparser.AlterTable, err error) {
	hashExists := map[string]bool{}
	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.DropKey:
			if node.Type == sqlparser.CheckKeyType {
				// drop a check constraint
				mappedName, ok := constraintMap[node.Name.String()]
				if !ok {
					return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Found DROP CONSTRAINT: %v, but could not find constraint name in map", sqlparser.CanonicalString(node))
				}
				node.Name = sqlparser.NewIdentifierCI(mappedName)
			}
		case *sqlparser.AddConstraintDefinition:
			oldName := node.ConstraintDefinition.Name.String()
			newName := e.newConstraintName(onlineDDL, GetConstraintType(node.ConstraintDefinition.Details), hashExists, sqlparser.CanonicalString(node.ConstraintDefinition.Details), oldName)
			node.ConstraintDefinition.Name = sqlparser.NewIdentifierCI(newName)
			constraintMap[oldName] = newName
		}
		return true, nil
	}
	if err := sqlparser.Walk(validateWalk, alterTable); err != nil {
		return alters, err
	}
	alters = append(alters, alterTable)
	// Handle ADD FULLTEXT KEY statements
	countAddFullTextStatements := 0
	redactedOptions := make([]sqlparser.AlterOption, 0, len(alterTable.AlterOptions))
	for i := range alterTable.AlterOptions {
		opt := alterTable.AlterOptions[i]
		switch opt := opt.(type) {
		case *sqlparser.AddIndexDefinition:
			if opt.IndexDefinition.Info.Fulltext {
				countAddFullTextStatements++
				if countAddFullTextStatements > 1 {
					// We've already got one ADD FULLTEXT KEY. We can't have another
					// in the same statement
					extraAlterTable := &sqlparser.AlterTable{
						Table:        alterTable.Table,
						AlterOptions: []sqlparser.AlterOption{opt},
					}
					alters = append(alters, extraAlterTable)
					continue
				}
			}
		}
		redactedOptions = append(redactedOptions, opt)
	}
	alterTable.AlterOptions = redactedOptions
	return alters, nil
}

// createTableLike creates the table named by `newTableName` in the likeness of onlineDDL.Table
// This function emulates MySQL's `CREATE TABLE LIKE ...` statement. The difference is that this function takes control over the generated CONSTRAINT names,
// if any, such that they are detrministic across shards, as well as preserve original names where possible.
func (e *Executor) createTableLike(ctx context.Context, newTableName string, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (constraintMap map[string]string, err error) {
	existingShowCreateTable, err := e.showCreateTable(ctx, onlineDDL.Schema, onlineDDL.Table)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in createTableLike(), newTableName=%s", newTableName)
	}
	stmt, err := sqlparser.ParseStrictDDL(existingShowCreateTable)
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CreateTable statement, got: %v", sqlparser.CanonicalString(stmt))
	}
	createTable.SetTable(createTable.GetTable().Qualifier.CompliantName(), newTableName)
	// manipulate CreateTable statement: take care of constraints names which have to be
	// unique across the schema
	constraintMap, err = e.validateAndEditCreateTableStatement(ctx, onlineDDL, createTable)
	if err != nil {
		return nil, err
	}
	// Create the table
	if _, err := conn.ExecuteFetch(sqlparser.CanonicalString(createTable), 0, false); err != nil {
		return nil, err
	}
	return constraintMap, nil
}

// initVreplicationOriginalMigration performs the first steps towards running a VRepl ALTER migration:
// - analyze the original table
// - formalize a new CreateTable statement
// - inspect the ALTER TABLE query
// - formalize an AlterTable statement
// - create the vrepl table
// - modify the vrepl table
// - Create and return a VRepl instance
func (e *Executor) initVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, conn *dbconnpool.DBConnection) (v *VRepl, err error) {
	restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
	defer restoreSQLModeFunc()
	if err != nil {
		return v, err
	}

	vreplTableName := fmt.Sprintf("_%s_%s_vrepl", onlineDDL.UUID, ReadableTimestamp())
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	constraintMap, err := e.createTableLike(ctx, vreplTableName, onlineDDL, conn)
	if err != nil {
		return nil, err
	}
	{
		stmt, err := sqlparser.ParseStrictDDL(onlineDDL.SQL)
		if err != nil {
			return nil, err
		}
		alterTable, ok := stmt.(*sqlparser.AlterTable)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected AlterTable statement, got: %v", sqlparser.CanonicalString(stmt))
		}
		// ALTER TABLE should apply to the vrepl table
		alterTable.SetTable(alterTable.GetTable().Qualifier.CompliantName(), vreplTableName)
		// Also, change any constraint names:
		alters, err := e.validateAndEditAlterTableStatement(ctx, onlineDDL, alterTable, constraintMap)
		if err != nil {
			return v, err
		}
		// Apply ALTER TABLE to materialized table
		for _, alter := range alters {
			if _, err := conn.ExecuteFetch(sqlparser.CanonicalString(alter), 0, false); err != nil {
				return v, err
			}
		}
	}
	v = NewVRepl(onlineDDL.UUID, onlineDDL.Schema, e.shard, onlineDDL.Schema, onlineDDL.Table, vreplTableName, onlineDDL.SQL)
	return v, nil
}

// postInitVreplicationOriginalMigration runs extra changes after a vreplication online DDL has been initialized.
// This function is called after both source and target tables have been analyzed, so there's more information
// about the two, and about the transition between the two.
func (e *Executor) postInitVreplicationOriginalMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, v *VRepl, conn *dbconnpool.DBConnection) (err error) {
	if v.sourceAutoIncrement > 0 && !v.parser.IsAutoIncrementDefined() {
		restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
		defer restoreSQLModeFunc()
		if err != nil {
			return err
		}

		// Apply ALTER TABLE AUTO_INCREMENT=?
		parsed := sqlparser.BuildParsedQuery(sqlAlterTableAutoIncrement, v.targetTable, ":auto_increment")
		bindVars := map[string]*querypb.BindVariable{
			"auto_increment": sqltypes.Uint64BindVariable(v.sourceAutoIncrement),
		}
		bound, err := parsed.GenerateQuery(bindVars, nil)
		if err != nil {
			return err
		}
		if _, err := conn.ExecuteFetch(bound, 0, false); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) initVreplicationRevertMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) (v *VRepl, err error) {
	// Getting here we've already validated that migration is revertible

	// Validation: vreplication still exists for reverted migration
	revertStream, err := e.readVReplStream(ctx, revertMigration.UUID, false)
	if err != nil {
		// cannot read the vreplication stream which we want to revert
		return nil, fmt.Errorf("can not revert vreplication migration %s because vreplication stream %s was not found", revertMigration.UUID, revertMigration.UUID)
	}

	onlineDDL.Table = revertMigration.Table
	if err := e.updateMySQLTable(ctx, onlineDDL.UUID, onlineDDL.Table); err != nil {
		return nil, err
	}

	vreplTableName, err := getVreplTable(ctx, revertStream)
	if err != nil {
		return nil, err
	}

	if err := e.updateArtifacts(ctx, onlineDDL.UUID, vreplTableName); err != nil {
		return v, err
	}
	v = NewVRepl(onlineDDL.UUID, onlineDDL.Schema, e.shard, onlineDDL.Schema, onlineDDL.Table, vreplTableName, "")
	v.pos = revertStream.pos
	return v, nil
}

// ExecuteWithVReplication sets up the grounds for a vreplication schema migration
func (e *Executor) ExecuteWithVReplication(ctx context.Context, onlineDDL *schema.OnlineDDL, revertMigration *schema.OnlineDDL) error {
	// make sure there's no vreplication workflow running under same name
	_ = e.terminateVReplMigration(ctx, onlineDDL.Schema, onlineDDL.UUID)

	if conflictFound, conflictingMigration := e.isAnyConflictingMigrationRunning(onlineDDL); conflictFound {
		return vterrors.Wrapf(ErrExecutorMigrationAlreadyRunning, "conflicting migration: %v over table: %v", conflictingMigration.UUID, conflictingMigration.Table)
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return ErrExecutorNotWritableTablet
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	_, err = conn.ExecuteFetch(fmt.Sprintf("use %s", onlineDDL.Schema), 1000, false)
	if err != nil {
		return err
	}
	defer conn.Close()

	e.ownedRunningMigrations.Store(onlineDDL.UUID, onlineDDL)
	if err := e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint); err != nil {
		return err
	}

	var v *VRepl
	if revertMigration == nil {
		// Original ALTER TABLE request for vreplication
		v, err = e.initVreplicationOriginalMigration(ctx, onlineDDL, conn)
	} else {
		// this is a revert request
		v, err = e.initVreplicationRevertMigration(ctx, onlineDDL, revertMigration)
	}
	if err != nil {
		return err
	}
	if err := v.analyze(ctx, conn); err != nil {
		return err
	}
	if err := e.updateMigrationTableRows(ctx, onlineDDL.UUID, v.tableRows); err != nil {
		return err
	}
	removedUniqueKeyNames := []string{}
	for _, uniqueKey := range v.removedUniqueKeys {
		removedUniqueKeyNames = append(removedUniqueKeyNames, uniqueKey.Name)
	}

	if err := e.updateSchemaAnalysis(ctx, onlineDDL.UUID,
		len(v.addedUniqueKeys),
		len(v.removedUniqueKeys),
		strings.Join(sqlescape.EscapeIDs(removedUniqueKeyNames), ","),
		strings.Join(sqlescape.EscapeIDs(v.droppedNoDefaultColumnNames), ","),
		strings.Join(sqlescape.EscapeIDs(v.expandedColumnNames), ","),
		v.revertibleNotes,
	); err != nil {
		return err
	}
	if revertMigration == nil {
		// Original ALTER TABLE request for vreplication
		if err := e.validateTableForAlterAction(ctx, onlineDDL); err != nil {
			return err
		}
		if err := e.postInitVreplicationOriginalMigration(ctx, onlineDDL, v, conn); err != nil {
			return err
		}
	}

	{
		// We need to talk to tabletmanager's VREngine. But we're on TabletServer. While we live in the same
		// process as VREngine, it is actually simpler to get hold of it via gRPC, just like wrangler does.
		tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
		if err != nil {
			return err
		}

		// reload schema before migration
		if err := e.reloadSchema(ctx); err != nil {
			return err
		}

		// create vreplication entry
		insertVReplicationQuery, err := v.generateInsertStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tablet.Tablet, insertVReplicationQuery); err != nil {
			return err
		}

		{
			// temporary hack. todo: this should be done when inserting any mysql.vreplication record across all workflow types
			query := fmt.Sprintf("update mysql.vreplication set workflow_type = %d where workflow = '%s'",
				binlogdatapb.VReplicationWorkflowType_OnlineDDL, v.workflow)
			if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
				return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", tablet.Tablet, query)
			}
		}
		// start stream!
		startVReplicationQuery, err := v.generateStartStatement(ctx)
		if err != nil {
			return err
		}
		if _, err := e.vreplicationExec(ctx, tablet.Tablet, startVReplicationQuery); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) readMigration(ctx context.Context, uuid string) (onlineDDL *schema.OnlineDDL, row sqltypes.RowNamedValues, err error) {

	parsed := sqlparser.BuildParsedQuery(sqlSelectMigration, ":migration_uuid")
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return onlineDDL, nil, err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	if err != nil {
		return onlineDDL, nil, err
	}
	row = r.Named().Row()
	if row == nil {
		// No results
		return nil, nil, ErrMigrationNotFound
	}
	onlineDDL = &schema.OnlineDDL{
		Keyspace:         row["keyspace"].ToString(),
		Table:            row["mysql_table"].ToString(),
		Schema:           row["mysql_schema"].ToString(),
		SQL:              row["migration_statement"].ToString(),
		UUID:             row["migration_uuid"].ToString(),
		Strategy:         schema.DDLStrategy(row["strategy"].ToString()),
		Options:          row["options"].ToString(),
		Status:           schema.OnlineDDLStatus(row["migration_status"].ToString()),
		Retries:          row.AsInt64("retries", 0),
		ReadyToComplete:  row.AsInt64("ready_to_complete", 0),
		TabletAlias:      row["tablet"].ToString(),
		MigrationContext: row["migration_context"].ToString(),
	}
	return onlineDDL, row, nil
}

func (e *Executor) deleteMigration(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlDeleteOnlineDDL, ":migration_uuid")
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	if err != nil {
		return err
	}
	return nil
}

// readPendingMigrationsUUIDs returns UUIDs for migrations in pending state (queued/ready/running)
func (e *Executor) readPendingMigrationsUUIDs(ctx context.Context) (uuids []string, err error) {
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectPendingMigrations)
	if err != nil {
		return uuids, err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		uuids = append(uuids, uuid)
	}
	return uuids, err
}

// terminateMigration attempts to interrupt and hard-stop a running migration
func (e *Executor) terminateMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) (foundRunning bool, err error) {
	log.Infof("terminateMigration: request to terminate %s", onlineDDL.UUID)
	// It's possible the killing the migration fails for whatever reason, in which case
	// the logic will retry killing it later on.
	// Whatever happens in this function, this executor stops owning the given migration.
	defer e.ownedRunningMigrations.Delete(onlineDDL.UUID)

	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
		// migration could have started by a different tablet. We need to actively verify if it is running
		s, _ := e.readVReplStream(ctx, onlineDDL.UUID, true)
		foundRunning = (s != nil && s.isRunning())
		if err := e.terminateVReplMigration(ctx, onlineDDL.Schema, onlineDDL.UUID); err != nil {
			return foundRunning, fmt.Errorf("Error terminating migration, vreplication exec error: %+v", err)
		}
	}
	return foundRunning, nil
}

func (e *Executor) stopVReplMigration(ctx context.Context, tableSchema string, uuid string) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlPauseVReplStream,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	// silently skip error; stopping the stream is just a graceful act; later deleting it is more important
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		log.Errorf("FAIL vreplicationExec: uuid=%s, query=%v, error=%v", uuid, query, err)
	}

	return err
}

// set Vreplication state to the value of state_before_pause, and clear the value of state_before_pause
func (e *Executor) continueVReplMigration(ctx context.Context, tableSchema string, uuid string) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlContinueVReplStream,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		log.Errorf("FAIL vreplicationExec: uuid=%s, query=%v, error=%v", uuid, query, err)
	}
	return err
}

func (e *Executor) clearStateBeforePauseOfVReplMigration(ctx context.Context, tableSchema string, uuid string) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlClearVReplStreamStateBeforePause,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		log.Errorf("FAIL vreplicationExec: uuid=%s, query=%v, error=%v", uuid, query, err)
	}

	return err
}

func (e *Executor) updateMigrationStatusPaused(ctx context.Context, uuid string, statusBeforePaused schema.OnlineDDLStatus) error {
	log.Infof("updateMigrationStatusPaused: transitioning migration: %s into status paused", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusPaused,
		sqltypes.StringBindVariable(string(statusBeforePaused)),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) isVreplAlreadyStopBeforePause(ctx context.Context, tableSchema string, uuid string) (bool, error) {
	query, err := sqlparser.ParseAndBind(sqlSelectVReplStreamStateBeforePause,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return false, err
	}
	rst, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return false, err
	}
	if len(rst.Named().Rows) != 1 {
		return false, errors.New("the num of vreplication entry is not 1")
	}
	if rst.Named().Rows[0]["state_before_pause"].ToString() == "Stopped" {
		return true, nil
	}
	return false, nil
}

func (e *Executor) updateMigrationStatusReady(ctx context.Context, uuid string) error {
	log.Infof("updateMigrationStatusReady: transitioning migration: %s into status ready", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusReady,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationStatusAndClearStatusBeforePaused(ctx context.Context, status schema.OnlineDDLStatus, uuid string) error {
	log.Infof("updateMigrationStatusQueued: transitioning migration: %s into status queued", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusAndClearStatusBeforePaused,
		sqltypes.StringBindVariable(string(status)),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationStatusQueued(ctx context.Context, uuid string) error {
	log.Infof("updateMigrationStatusQueued: transitioning migration: %s into status queued", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusQueued,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) clearMigrationStatusBeforePausedAndSetRunning(ctx context.Context, uuid string) error {
	log.Infof("clearMigrationStatusBeforePausedAndSetRunning: set status before paused of migration: %s as NULL, and set status as 'running'", uuid)
	query, err := sqlparser.ParseAndBind(sqlClearMigrationStatusBeforeAndSetRunning,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) readMigrationStatusBeforePaused(ctx context.Context, uuid string) (status schema.OnlineDDLStatus, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlSelectMigrationStatusBeforePause, ":migration_uuid")
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	if err != nil {
		return "", err
	}
	row := r.Named().Row()
	if row == nil {
		// No results
		return "", ErrMigrationNotFound
	}

	return schema.OnlineDDLStatus(row["status_before_paused"].ToString()), nil
}

// pause an online DDL migration with uuid, this function simply do some legality check,
// then it stop the vreplication if migration status is 'running', and set migration status to 'paused'
func (e *Executor) PauseMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	log.Infof("PauseMigration: request to pause %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	onlineDDL, _, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}

	if onlineDDL.Strategy != schema.DDLStrategyOnline && onlineDDL.Strategy != schema.DDLStrategyVitess {
		log.Infof("PauseMigration: migration %s strategy is not online or vitess: strategy %v and can not be paused", uuid, onlineDDL.Strategy)
		return emptyResult, nil
	}

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled, schema.OnlineDDLStatusPaused:
		log.Infof("PauseMigration: migration %s is in status: %v and can not be paused", uuid, onlineDDL.Status)
		return emptyResult, nil
	case schema.OnlineDDLStatusRunning:
		// need to stop vreplication
		if err = e.stopVReplMigration(ctx, onlineDDL.Schema, onlineDDL.UUID); err != nil {
			return emptyResult, err
		}
		// Determine whether the vreplication is already in the "Stopped" state when pausing by reading the 'state_before_pause' field.
		// If so, directly set 'state_before_pause' to NULL, and return.
		// Actually, the code within { } below will not run if there is no error occurs during vreplication cut over.
		// Because when the onlineDDL scheduler changes the state of vreplication to 'Stopped', it still holds the migrationMutex lock.
		// And the lock will not be released until the status of onlieDDL migration is changed to complete at the end of vreplication cut over.
		// Therefore, if no error occurs, when this function holds the lock, it is impossible for the status of migration to be 'running' and the state of vreplication to be 'Stopped' at the same time.
		// But if an error occurs after vreplication is stopped but before the migration is completed, this may occur.
		{
			alreadyStopped := false
			if alreadyStopped, err = e.isVreplAlreadyStopBeforePause(ctx, onlineDDL.Schema, onlineDDL.UUID); err != nil {
				return emptyResult, err
			}
			if alreadyStopped {
				if err = e.clearStateBeforePauseOfVReplMigration(ctx, onlineDDL.Schema, onlineDDL.UUID); err != nil {
					log.Warningf("clearStateBeforePauseOfVReplMigration failed, table schema:%s uuid:%s", onlineDDL.Schema, onlineDDL.UUID)
					return emptyResult, err
				}
				log.Infof("the Vrepl is already stopped so the onlineDDL %s can not be paused now", onlineDDL.UUID)
				return emptyResult, err // emptyResult represents no migration is paused
			}
		}
		e.ownedRunningMigrations.Delete(onlineDDL.UUID)
	}

	if err = e.updateMigrationStatusPaused(ctx, onlineDDL.UUID, onlineDDL.Status); err != nil {
		return emptyResult, err
	}

	return &sqltypes.Result{RowsAffected: 1}, nil

}

// set migration status based on status before paused
// if 'queued' before paused, then set to 'queued'
// if 'ready' before paused, then set to 'ready'
// if 'running' before paused, then set to 'ready'
func (e *Executor) ResumeMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	log.Infof("ResumeMigration: request to cancel %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	onlineDDL, _, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}
	if onlineDDL.Status != schema.OnlineDDLStatusPaused {
		log.Infof("can not unpause migration %s which is in %s state", uuid, onlineDDL.Status)
		return emptyResult, nil
	}

	statusBeforePaused, err := e.readMigrationStatusBeforePaused(ctx, uuid)
	if err != nil {
		return emptyResult, err
	}
	switch statusBeforePaused {
	case schema.OnlineDDLStatusQueued:
		if err = e.updateMigrationStatusAndClearStatusBeforePaused(ctx, schema.OnlineDDLStatusQueued, uuid); err != nil {
			return emptyResult, err
		}
	case schema.OnlineDDLStatusReady:
		if err = e.updateMigrationStatusAndClearStatusBeforePaused(ctx, schema.OnlineDDLStatusReady, uuid); err != nil {
			return emptyResult, err
		}
	case schema.OnlineDDLStatusRunning:
		if err = e.updateMigrationStatusReady(ctx, uuid); err != nil {
			return emptyResult, err
		}
	}

	return &sqltypes.Result{RowsAffected: 1}, nil
}

func (e *Executor) PauseAllMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("CancelPendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("PauseAllMigrations: pausing %s", uuid)
		res, err := e.PauseMigration(ctx, uuid)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("PauseAllMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

func (e *Executor) ResumeAllMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	pausedMigrations, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectMigrationsPaused)
	if err != nil {
		return result, err
	}
	result = &sqltypes.Result{}
	for _, row := range pausedMigrations.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		log.Infof("ResumeAllMigrations: unpausing %s", uuid)
		res, err := e.ResumeMigration(ctx, uuid)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("ResumeAllMigrations: done iterating all paused migrations")
	return result, nil
}

// CancelMigration attempts to abort a scheduled or a running migration
func (e *Executor) CancelMigration(ctx context.Context, uuid string, message string, issuedByUser bool) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	log.Infof("CancelMigration: request to cancel %s with message: %v", uuid, message)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var rowsAffected uint64

	onlineDDL, _, err := e.readMigration(ctx, uuid)
	if err != nil {
		return nil, err
	}

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled:
		log.Infof("CancelMigration: migration %s is in non-cancellable status: %v", uuid, onlineDDL.Status)
		return emptyResult, nil
	}
	// From this point on, we're actually cancelling a migration
	if issuedByUser {
		// if this was issued by the user, then we mark the `cancelled_timestamp`, and based on that,
		// the migration state will be 'cancelled'.
		// If this was not issued by the user, then this is an internal state machine cancellation of the
		// migration, e.g. because it is stale or has an unrecoverable error. In this case we do not mark
		// the timestamp, and as result, the state will transition to 'failed'
		if err := e.updateMigrationTimestamp(ctx, "cancelled_timestamp", uuid); err != nil {
			return nil, err
		}
	}
	defer e.failMigration(ctx, onlineDDL, errors.New(message))
	defer e.triggerNextCheckInterval()

	switch onlineDDL.Status {
	case schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady:
		log.Infof("CancelMigration: cancelling %s with status: %v", uuid, onlineDDL.Status)
		return &sqltypes.Result{RowsAffected: 1}, nil
	}

	migrationFound, err := e.terminateMigration(ctx, onlineDDL)
	if migrationFound {
		log.Infof("CancelMigration: terminated %s with status: %v", uuid, onlineDDL.Status)
		rowsAffected = 1
	} else {
		log.Infof("CancelMigration: migration %s wasn't found to be running", uuid)
	}
	if err != nil {
		return result, err
	}

	result = &sqltypes.Result{
		RowsAffected: rowsAffected,
	}
	return result, nil
}

// cancelMigrations attempts to abort a list of migrations
func (e *Executor) cancelMigrations(ctx context.Context, cancellable []*cancellableMigration, issuedByUser bool) (err error) {
	for _, migration := range cancellable {
		log.Infof("cancelMigrations: cancelling %s; reason: %s", migration.uuid, migration.message)
		if _, err := e.CancelMigration(ctx, migration.uuid, migration.message, issuedByUser); err != nil {
			return err
		}
	}
	return nil
}

// CancelPendingMigrations cancels all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) CancelPendingMigrations(ctx context.Context, message string, issuedByUser bool) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("CancelPendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("CancelPendingMigrations: cancelling %s", uuid)
		res, err := e.CancelMigration(ctx, uuid, message, issuedByUser)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("CancelPendingMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

func (e *Executor) validateThrottleParams(ctx context.Context, expireString string, ratioLiteral *sqlparser.Literal) (duration time.Duration, ratio float64, err error) {
	duration = time.Hour * 24 * 365 * 100
	if expireString != "" {
		duration, err = time.ParseDuration(expireString)
		if err != nil || duration < 0 {
			return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid EXPIRE value: %s. Try '120s', '30m', '1h', etc. Allowed units are (s)ec, (m)in, (h)hour", expireString)
		}
	}
	ratio = 1.0
	if ratioLiteral != nil {
		ratio, err = strconv.ParseFloat(ratioLiteral.Val, 64)
		if err != nil || ratio < 0 || ratio > 1 {
			return duration, ratio, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid RATIO value: %s. Try any decimal number between '0.0' (no throttle) and `1.0` (fully throttled)", ratioLiteral.Val)
		}
	}
	return duration, ratio, nil
}

// ThrottleMigration
func (e *Executor) ThrottleMigration(ctx context.Context, uuid string, expireString string, ratioLiteral *sqlparser.Literal) (result *sqltypes.Result, err error) {
	duration, ratio, err := e.validateThrottleParams(ctx, expireString, ratioLiteral)
	if err != nil {
		return nil, err
	}
	if err := e.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	_ = e.lagThrottler.ThrottleApp(uuid, time.Now().Add(duration), ratio)
	return emptyResult, nil
}

// ThrottleAllMigrations
func (e *Executor) ThrottleAllMigrations(ctx context.Context, expireString string, ratioLiteral *sqlparser.Literal) (result *sqltypes.Result, err error) {
	duration, ratio, err := e.validateThrottleParams(ctx, expireString, ratioLiteral)
	if err != nil {
		return nil, err
	}
	if err := e.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	_ = e.lagThrottler.ThrottleApp(throttlerOnlineDDLApp, time.Now().Add(duration), ratio)
	return emptyResult, nil
}

// UnthrottleMigration
func (e *Executor) UnthrottleMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if err := e.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	_ = e.lagThrottler.UnthrottleApp(uuid)
	return emptyResult, nil
}

// UnthrottleAllMigrations
func (e *Executor) UnthrottleAllMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if err := e.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	_ = e.lagThrottler.UnthrottleApp(throttlerOnlineDDLApp)
	return emptyResult, nil
}

// scheduleNextMigration attemps to schedule a single migration to run next.
// possibly there are migrations to run.
// The effect of this function is to move a migration from 'queued' state to 'ready' state, is all.
func (e *Executor) scheduleNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	var onlyScheduleOneMigration sync.Once

	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectQueuedMigrations)
	if err != nil {
		return err
	}
	pausedMigrations, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectMigrationsPausedWhenQueued)
	if err != nil {
		return err
	}

	for _, row := range r.Named().Rows {
		id, _ := row["id"].ToInt64()
		uuid := row["migration_uuid"].ToString()
		table := row["mysql_table"].ToString()
		tableSchema := row["mysql_schema"].ToString()
		postponeLaunch := row.AsBool("postpone_launch", false)
		postponeCompletion := row.AsBool("postpone_completion", false)
		readyToComplete := row.AsBool("ready_to_complete", false)
		isImmediateOperation := row.AsBool("is_immediate_operation", false)

		// if there is one online DDL task with smaller id, the same table and was 'pasued'
		// then the current online DDL should be blocked for the sake of correctness
		migrationQueuedBeforeIsPaused := false
		for _, pausedMigrationRow := range pausedMigrations.Named().Rows {

			pausedMigrationID, _ := pausedMigrationRow["id"].ToInt64()
			pausedMigrationTable := pausedMigrationRow["mysql_table"].ToString()
			pausedMigrationSchema := pausedMigrationRow["mysql_schema"].ToString()

			if pausedMigrationID < id && pausedMigrationTable == table && tableSchema == pausedMigrationSchema {
				migrationQueuedBeforeIsPaused = true
				break
			}
		}
		if migrationQueuedBeforeIsPaused {
			continue
		}

		if postponeLaunch {
			// We don't even look into this migration until its postpone_launch flag is cleared
			continue
		}

		if !readyToComplete {
			// see if we need to update ready_to_complete
			if isImmediateOperation {
				// Whether postponsed or not, CREATE and DROP operations, as well as VIEW operations,
				// are inherently "ready to complete" because their operation is immediate.
				if err := e.updateMigrationReadyToComplete(ctx, uuid, true); err != nil {
					return err
				}
			}
		}

		if !(isImmediateOperation && postponeCompletion) {
			// Any non-postponed migration can be scheduled
			// postponed ALTER can be scheduled (because gh-ost or vreplication will postpone the cut-over)
			// We only schedule a single migration in the execution of this function
			onlyScheduleOneMigration.Do(func() {
				err = e.updateMigrationStatus(ctx, uuid, schema.OnlineDDLStatusReady)
				log.Infof("Executor.scheduleNextMigration: scheduling migration %s; err: %v", uuid, err)
				e.triggerNextCheckInterval()
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

// reviewEmptyTableRevertMigrations reviews a queued REVERT migration. Such a migration has the following SQL:
// "REVERT VITESS_MIGRATION '...'"
// There's nothing in this SQL to indicate:
// - which table is involved?
// - is this a table or a view?
// - Are we reverting a CREATE? A DROP? An ALTER?
// This function fills in the blanks and updates the database row.
func (e *Executor) reviewEmptyTableRevertMigrations(ctx context.Context, onlineDDL *schema.OnlineDDL) (changesMade bool, err error) {
	if onlineDDL.Table != "" {
		return false, nil
	}
	// Table name is empty. Let's populate it.

	// Try to update table name and ddl_action
	// Failure to do so fails the migration
	revertUUID, err := onlineDDL.GetRevertUUID()
	if err != nil {
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot analyze revert UUID for revert migration %s: %v", onlineDDL.UUID, err))
	}
	revertedMigration, revertedRow, err := e.readMigration(ctx, revertUUID)
	if err != nil {
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot read migration %s reverted by migration %s: %s", revertUUID, onlineDDL.UUID, err))
	}
	revertedActionStr := revertedRow["ddl_action"].ToString()

	mimickedActionStr := ""
	switch revertedActionStr {
	case sqlparser.CreateStr:
		mimickedActionStr = sqlparser.DropStr
	case sqlparser.DropStr:
		mimickedActionStr = sqlparser.CreateStr
	case sqlparser.AlterStr:
		mimickedActionStr = sqlparser.AlterStr
	default:
		return false, e.failMigration(ctx, onlineDDL, fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertedMigration.UUID, revertedActionStr))
	}
	if err := e.updateDDLAction(ctx, onlineDDL.UUID, mimickedActionStr); err != nil {
		return false, err
	}
	if err := e.updateMigrationIsView(ctx, onlineDDL.UUID, revertedRow.AsBool("is_view", false)); err != nil {
		return false, err
	}
	if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertedMigration.Table); err != nil {
		return false, err
	}
	return true, nil
}

// reviewImmediateOperations reviews a queued migration and determines whether it is an "immediate operation".
// Immediate operations are ones that can be performed within a split second, or rather, do not require long
// running processes. Immediate operations are:
// - CREATE TABLE
// - DROP TABLE (which we convert into RENAME)
// - All VIEW operations
// - An INSTANT DDL accompanied by relevant ddl strategy flags
// Non immediate operations are:
// - A gh-ost migration
// - A vitess (vreplication) migration
func (e *Executor) reviewImmediateOperations(ctx context.Context, capableOf mysql.CapableOf, onlineDDL *schema.OnlineDDL, ddlAction string, isRevert bool, isView bool) (bool, error) {
	switch ddlAction {
	case sqlparser.CreateStr, sqlparser.DropStr:
		return true, nil
	case sqlparser.AlterStr:
		switch {
		case isView:
			return true, nil
		case isRevert:
			// REVERT for a true ALTER TABLE. not an immediate operation
			return false, nil
		default:
			specialPlan, err := e.analyzeSpecialAlterPlan(ctx, onlineDDL, capableOf)
			if err != nil {
				return false, err
			}
			return (specialPlan != nil), nil
		}
	}
	return false, nil
}

// reviewQueuedMigrations iterates through queued migrations and sees if any information needs to be updated.
// The function analyzes the queued migration and fills in some blanks:
// - If this is a REVERT migration, what table is affected? What's the operation?
// - Is this migration an "immediate operation"?
func (e *Executor) reviewQueuedMigrations(ctx context.Context) error {
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	defer conn.Close()
	_, capableOf, _ := mysql.GetFlavor(conn.ServerVersion, nil)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectQueuedUnreviewedMigrations)
	if err != nil {
		return err
	}

	for _, uuidRow := range r.Named().Rows {
		uuid := uuidRow["migration_uuid"].ToString()
		onlineDDL, row, err := e.readMigration(ctx, uuid)
		if err != nil {
			return err
		}
		// handle REVERT migrations: populate table name and update ddl action and is_view:
		ddlAction := row["ddl_action"].ToString()
		isRevert := false
		if ddlAction == schema.RevertActionStr {
			isRevert = true
			rowModified, err := e.reviewEmptyTableRevertMigrations(ctx, onlineDDL)
			if err != nil {
				return err
			}
			if rowModified {
				// re-read migration and entire row
				onlineDDL, row, err = e.readMigration(ctx, uuid)
				if err != nil {
					return err
				}
				ddlAction = row["ddl_action"].ToString()
			}
		}
		isView := row.AsBool("is_view", false)
		isImmediate, err := e.reviewImmediateOperations(ctx, capableOf, onlineDDL, ddlAction, isRevert, isView)
		if err != nil {
			e.failMigration(ctx, onlineDDL, err)
			return err
		}
		if isImmediate {
			if err := e.updateMigrationSetImmediateOperation(ctx, onlineDDL.UUID); err != nil {
				e.failMigration(ctx, onlineDDL, err)
				return err
			}
		}
		// Find conditions where the migration cannot take place:
		switch onlineDDL.Strategy {
		case schema.DDLStrategyMySQL:
			strategySetting := onlineDDL.StrategySetting()
			if strategySetting.IsPostponeCompletion() {
				e.failMigration(ctx, onlineDDL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--postpone-completion not supported in 'mysql' strategy"))
			}
			if strategySetting.IsAllowZeroInDateFlag() {
				e.failMigration(ctx, onlineDDL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--allow-zero-in-date not supported in 'mysql' strategy"))
			}
		}

		// The review is complete. We've backfilled details on the migration row. We mark
		// the migration as having been reviewed. The function scheduleNextMigration() will then
		// have access to this row.
		if err := e.updateMigrationTimestamp(ctx, "reviewed_timestamp", uuid); err != nil {
			return err
		}

	}
	return nil
}

func (e *Executor) validateMigrationRevertible(ctx context.Context, revertMigration *schema.OnlineDDL, revertingMigrationUUID string) (err error) {
	// Validation: migration to revert exists and is in complete state
	action, actionStr, err := revertMigration.GetActionStr()
	if err != nil {
		return err
	}
	switch action {
	case sqlparser.AlterDDLAction:
		if revertMigration.Strategy != schema.DDLStrategyOnline && revertMigration.Strategy != schema.DDLStrategyVitess {
			return fmt.Errorf("can only revert a %s strategy migration. Migration %s has %s strategy", schema.DDLStrategyOnline, revertMigration.UUID, revertMigration.Strategy)
		}
	case sqlparser.RevertDDLAction:
	case sqlparser.CreateDDLAction:
	case sqlparser.DropDDLAction:
	default:
		return fmt.Errorf("cannot revert migration %s: unexpected action %s", revertMigration.UUID, actionStr)
	}
	if revertMigration.Status != schema.OnlineDDLStatusComplete {
		return fmt.Errorf("can only revert a migration in a '%s' state. Migration %s is in '%s' state", schema.OnlineDDLStatusComplete, revertMigration.UUID, revertMigration.Status)
	}
	{
		// Validation: see if there's a pending migration on this table:
		r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectPendingMigrations)
		if err != nil {
			return err
		}
		// we identify running migrations on requested table
		for _, row := range r.Named().Rows {
			pendingUUID := row["migration_uuid"].ToString()
			if pendingUUID == revertingMigrationUUID {
				// that's fine; the migration we're looking at is the very one that's trying to issue this revert
				continue
			}
			keyspace := row["mysql_schema"].ToString()
			table := row["mysql_table"].ToString()
			status := schema.OnlineDDLStatus(row["migration_status"].ToString())

			if keyspace == revertMigration.Schema && table == revertMigration.Table {
				return fmt.Errorf("can not revert migration %s on table %s because migration %s is in %s status. May only revert if all migrations on this table are completed or failed", revertMigration.UUID, revertMigration.Table, pendingUUID, status)
			}
		}
		{
			// Validation: see that we're reverting the last successful migration on this table:
			query, err := sqlparser.ParseAndBind(sqlSelectCompleteMigrationsOnTable,
				sqltypes.StringBindVariable(revertMigration.Schema),
				sqltypes.StringBindVariable(revertMigration.Table),
			)
			if err != nil {
				return err
			}
			r, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
			if err != nil {
				return err
			}
			for _, row := range r.Named().Rows {
				completeUUID := row["migration_uuid"].ToString()
				if completeUUID != revertMigration.UUID {
					return fmt.Errorf("can not revert migration %s on table %s because it is not the last migration to complete on that table. The last migration to complete was %s", revertMigration.UUID, revertMigration.Table, completeUUID)
				}
			}
		}
	}
	return nil
}

// executeRevert is called for 'revert' migrations (SQL is of the form "revert 99caeca2_74e2_11eb_a693_f875a4d24e90", not a real SQL of course).
// In this function we:
// - figure out whether the revert is valid: can we really revert requested migration?
// - what type of migration we're reverting? (CREATE/DROP/ALTER)
// - revert appropriately to the type of migration
func (e *Executor) executeRevert(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	revertUUID, err := onlineDDL.GetRevertUUID()
	if err != nil {
		return fmt.Errorf("cannot run a revert migration %v: %+v", onlineDDL.UUID, err)
	}

	revertMigration, row, err := e.readMigration(ctx, revertUUID)
	if err != nil {
		return err
	}
	if err := e.validateMigrationRevertible(ctx, revertMigration, onlineDDL.UUID); err != nil {
		return err
	}
	revertedActionStr := row["ddl_action"].ToString()
	if onlineDDL.Table == "" {
		// table name should be populated by reviewQueuedMigrations
		// but this was a newly added functionality. To be backwards compatible,
		// we double check here, and populate table name and ddl_action.

		// TODO: remove in v14
		mimickedActionStr := ""

		switch revertedActionStr {
		case sqlparser.CreateStr:
			mimickedActionStr = sqlparser.DropStr
		case sqlparser.DropStr:
			mimickedActionStr = sqlparser.CreateStr
		case sqlparser.AlterStr:
			mimickedActionStr = sqlparser.AlterStr
		default:
			return fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertMigration.UUID, revertedActionStr)
		}
		if err := e.updateDDLAction(ctx, onlineDDL.UUID, mimickedActionStr); err != nil {
			return err
		}
		if err := e.updateMySQLTable(ctx, onlineDDL.UUID, revertMigration.Table); err != nil {
			return err
		}
	}

	switch revertedActionStr {
	case sqlparser.CreateStr:
		{
			// We are reverting a CREATE migration. The revert is to DROP, only we don't actually
			// drop the table, we rename it into lifecycle
			// Possibly this was a CREATE TABLE IF NOT EXISTS, and possibly the table already existed
			// before the DDL, in which case the CREATE was a noop. In that scenario we _do not_ drop
			// the table.
			// We can tell the difference by looking at the artifacts. A successful CREATE TABLE, where
			// a table actually gets created, has a sentry, dummy artifact. A noop has not.

			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// This indicates no table was actually created. this must have been a CREATE TABLE IF NOT EXISTS where the table already existed.
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			}

			for _, artifactTable := range artifactTables {
				if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
					return err
				}
				onlineDDL.SQL = sqlparser.BuildParsedQuery(sqlRenameTable, revertMigration.Table, artifactTable).Query
				if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
					return err
				}
			}
		}
	case sqlparser.DropStr:
		{
			// We are reverting a DROP migration. But the table wasn't really dropped, because that's not how
			// we run DROP migrations. It was renamed. So we need to rename it back.
			// But we impose as if we are now CREATE-ing the table.

			artifacts := row["artifacts"].ToString()
			artifactTables := textutil.SplitDelimitedList(artifacts)
			if len(artifactTables) > 1 {
				return fmt.Errorf("cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
			}
			if len(artifactTables) == 0 {
				// Could happen on `DROP TABLE IF EXISTS` where the table did not exist...
				_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			}
			for _, artifactTable := range artifactTables {
				if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
					return err
				}
				onlineDDL.SQL = sqlparser.BuildParsedQuery(sqlRenameTable, artifactTable, revertMigration.Table).Query
				if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
					return err
				}
			}
		}
	case sqlparser.AlterStr:
		{
			if row.AsBool("is_view", false) {
				artifacts := row["artifacts"].ToString()
				artifactTables := textutil.SplitDelimitedList(artifacts)
				if len(artifactTables) > 1 {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot run migration %s reverting %s: found %d artifact tables, expected maximum 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
				}
				if len(artifactTables) == 0 {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot run migration %s reverting %s: found %d artifact tables, expected 1", onlineDDL.UUID, revertMigration.UUID, len(artifactTables))
				}
				for _, artifactTable := range artifactTables {
					if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTable); err != nil {
						return err
					}
					onlineDDL.SQL, _, err = e.generateSwapTablesStatement(ctx, onlineDDL.Table, artifactTable)
					if err != nil {
						return err
					}
					if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
						return err
					}
				}
				return nil
			}
			// Real table
			if err := e.ExecuteWithVReplication(ctx, onlineDDL, revertMigration); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("cannot run migration %s reverting %s: unexpected action %s", onlineDDL.UUID, revertMigration.UUID, revertedActionStr)
	}

	return nil
}

// evaluateDeclarativeDiff is called for -declarative CREATE statements, where the table already exists. The function generates a SQL diff, which can be:
// - empty, in which case the migration is noop and implicitly successful, or
// - non-empty, in which case the migration turns to be an ALTER
func (e *Executor) evaluateDeclarativeDiff(ctx context.Context, onlineDDL *schema.OnlineDDL) (diff schemadiff.EntityDiff, err error) {

	// Modify the CREATE TABLE statement to indicate a different, made up table name, known as the "comparison table"
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return nil, err
	}
	// Is this CREATE TABLE or CREATE VIEW?
	comparisonTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return nil, err
	}

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecuteFetch(fmt.Sprintf("use %s", onlineDDL.Schema), 1000, false)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	{
		// Create the comparison table
		ddlStmt.SetTable("", comparisonTableName)
		modifiedCreateSQL := sqlparser.String(ddlStmt)

		restoreSQLModeFunc, err := e.initMigrationSQLMode(ctx, onlineDDL, conn)
		defer restoreSQLModeFunc()
		if err != nil {
			return nil, err
		}

		if _, err := conn.ExecuteFetch(modifiedCreateSQL, 0, false); err != nil {
			return nil, err
		}

		defer func() {
			// Drop the comparison table
			parsed := sqlparser.BuildParsedQuery(sqlDropTable, comparisonTableName)
			_, _ = conn.ExecuteFetch(parsed.Query, 0, false)
			// Nothing bad happens for not checking the error code. The table is GC/HOLD. If we
			// can't drop it now, it still gets collected later by tablegc mechanism
		}()
	}

	existingShowCreateTable, err := e.showCreateTable(ctx, onlineDDL.Schema, onlineDDL.Table)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in evaluateDeclarativeDiff(), for onlineDDL.Table")
	}
	if existingShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "unexpected: cannot find table or view %v", onlineDDL.Table)
	}
	newShowCreateTable, err := e.showCreateTable(ctx, onlineDDL.Schema, comparisonTableName)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in evaluateDeclarativeDiff(), for comparisonTableName")
	}
	if newShowCreateTable == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected: cannot find table or view even as it was just created: %v", onlineDDL.Table)
	}
	hints := &schemadiff.DiffHints{AutoIncrementStrategy: schemadiff.AutoIncrementApplyHigher}
	switch ddlStmt.(type) {
	case *sqlparser.CreateTable:
		diff, err = schemadiff.DiffCreateTablesQueries(existingShowCreateTable, newShowCreateTable, hints)
	case *sqlparser.CreateView:
		diff, err = schemadiff.DiffCreateViewsQueries(existingShowCreateTable, newShowCreateTable, hints)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expected CREATE TABLE or CREATE VIEW in online DDL statement: %v", onlineDDL.SQL)
	}
	if err != nil {
		return nil, err
	}
	return diff, nil
}

// getCompletedMigrationByContextAndSQL chceks if there exists a completed migration with exact same
// context and SQL as given migration. If so, it returns its UUID.
func (e *Executor) getCompletedMigrationByContextAndSQL(ctx context.Context, onlineDDL *schema.OnlineDDL) (completedUUID string, err error) {
	if onlineDDL.MigrationContext == "" {
		// only applies to migrations with an explicit context
		return "", nil
	}
	query, err := sqlparser.ParseAndBind(sqlSelectCompleteMigrationsByContextAndSQL,
		sqltypes.StringBindVariable(onlineDDL.Schema),
		sqltypes.StringBindVariable(onlineDDL.MigrationContext),
		sqltypes.StringBindVariable(onlineDDL.SQL),
	)
	if err != nil {
		return "", err
	}
	r, err := e.execQuery(ctx, onlineDDL.Schema, query)
	if err != nil {
		return "", err
	}
	for _, row := range r.Named().Rows {
		completedUUID = row["migration_uuid"].ToString()
	}
	return completedUUID, nil
}

// failMigration marks a migration as failed
func (e *Executor) failMigration(ctx context.Context, onlineDDL *schema.OnlineDDL, withError error) error {
	defer e.triggerNextCheckInterval()
	_ = e.updateMigrationStatusFailedOrCancelled(ctx, onlineDDL.UUID)
	if withError != nil {
		_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, withError.Error())
	}
	e.ownedRunningMigrations.Delete(onlineDDL.UUID)
	return withError
}

func (e *Executor) executeDropDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	// Drop statement.
	// Normally, we're going to modify DROP to RENAME (see later on). But if table name is
	// already a GC-lifecycle table, then we don't put it through yet another GC lifecycle,
	// we just drop it.
	if schema.IsGCTableName(onlineDDL.Table) {
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
		return nil
	}

	// We transform a DROP TABLE into a RENAME TABLE statement, so as to remove the table safely and asynchronously.

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}

	var toTableName string
	onlineDDL.SQL, toTableName, err = schema.GenerateRenameStatementWithUUID(onlineDDL.Schema, onlineDDL.Table, schema.HoldTableGCState, onlineDDL.GetGCUUID(), newGCTableRetainTime())
	if err != nil {
		return failMigration(err)
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, toTableName); err != nil {
		return err
	}

	acceptableErrorCodes := []int{}
	if ddlStmt.GetIfExists() {
		acceptableErrorCodes = acceptableDropTableIfExistsErrorCodes
	}
	acceptableErrCodeFound, err := e.executeDirectly(ctx, onlineDDL, acceptableErrorCodes...)
	if err != nil {
		return failMigration(err)
	}
	if acceptableErrCodeFound {
		// Table did not exist after all. There is no artifact
		if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) executeCreateDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}
	if _, isCreateView := ddlStmt.(*sqlparser.CreateView); isCreateView {
		if ddlStmt.GetIsReplace() {
			// This is a CREATE OR REPLACE VIEW
			exists, err := e.tableExists(ctx, onlineDDL.Schema, onlineDDL.Table)
			if err != nil {
				return failMigration(err)
			}
			if exists {
				// the view already exists. This CREATE OR REPLACE VIEW statement should
				// actually turn into an ALTER
				if err := e.executeAlterViewOnline(ctx, onlineDDL); err != nil {
					return failMigration(err)
				}
				return nil
			}
		}
	}
	// from now on, whether a VIEW or a TABLE, they get the same treatment

	sentryArtifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return failMigration(err)
	}
	// we create a dummy artifact. Its existence means the table was created by this migration.
	// It will be read by the revert operation.
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, sentryArtifactTableName); err != nil {
		return err
	}

	if ddlStmt.GetIfNotExists() {
		// This is a CREATE TABLE IF NOT EXISTS
		// We want to know if the table actually exists before running this migration.
		// If so, then the operation is noop, and when we revert the migration, we also do a noop.
		exists, err := e.tableExists(ctx, onlineDDL.Schema, onlineDDL.Table)
		if err != nil {
			return failMigration(err)
		}
		if exists {
			// the table already exists. This CREATE TABLE IF NOT EXISTS statement is a noop.
			// We therefore clear the artifact field. A revert operation will use this as a hint.
			if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
				return failMigration(err)
			}
		}
	}
	if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
		return failMigration(err)
	}
	return nil
}

// generateSwapTablesStatement creates a RENAME statement that swaps two tables, with assistance
// of temporary third table. It returns the name of generated third table, though normally
// that table should not exist before & after operation, only _during_ operation time.
func (e *Executor) generateSwapTablesStatement(ctx context.Context, tableName1, tableName2 string) (query string, swapTableName string, err error) {
	swapTableName, err = schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return "", swapTableName, err
	}
	parsed := sqlparser.BuildParsedQuery(sqlSwapTables,
		tableName1, swapTableName,
		tableName2, tableName1,
		swapTableName, tableName2,
	)
	return parsed.Query, swapTableName, nil
}

// renameTableIfApplicable renames a table, assuming it exists and that the target does not exist.
func (e *Executor) renameTableIfApplicable(ctx context.Context, tableSchema, fromTableName, toTableName string) (attemptMade bool, err error) {
	if fromTableName == "" {
		return false, nil
	}
	exists, err := e.tableExists(ctx, tableSchema, fromTableName)
	if err != nil {
		return false, err
	}
	if !exists {
		// can't rename from table when it does not exist
		return false, nil
	}
	exists, err = e.tableExists(ctx, tableSchema, toTableName)
	if err != nil {
		return false, err
	}
	if exists {
		// target table exists, abort.
		return false, nil
	}
	parsed := sqlparser.BuildParsedQuery(sqlRenameTable, fromTableName, toTableName)
	_, err = e.execQuery(ctx, tableSchema, parsed.Query)
	return true, err
}

func (e *Executor) executeAlterViewOnline(ctx context.Context, onlineDDL *schema.OnlineDDL) (err error) {
	artifactViewName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
	if err != nil {
		return err
	}
	stmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return err
	}
	switch viewStmt := stmt.(type) {
	case *sqlparser.CreateView:
		stmt.SetTable("", artifactViewName)
	case *sqlparser.AlterView:
		// consolidate the logic. We treat ALTER like we treat CREATE OR REPLACE
		// it actually easier for us to issue a CREATE OR REPLACE, because it
		// actually creates a view...
		stmt = &sqlparser.CreateView{
			Algorithm:   viewStmt.Algorithm,
			Definer:     viewStmt.Definer,
			Security:    viewStmt.Security,
			Columns:     viewStmt.Columns,
			Select:      viewStmt.Select,
			CheckOption: viewStmt.CheckOption,
			IsReplace:   true,
			Comments:    viewStmt.Comments,
		}
		stmt.SetTable("", artifactViewName)
	default:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "executeAlterViewOnline only supports CreateView and AlterView statements. Got: %v", sqlparser.String(viewStmt))
	}
	artifactViewCreateSQL := sqlparser.String(stmt)

	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return err
	}
	_, err = conn.ExecuteFetch(fmt.Sprintf("use %s", onlineDDL.Schema), 1000, false)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusRunning, false, progressPctStarted, etaSecondsUnknown, rowsCopiedUnknown, emptyHint)

	if _, err := conn.ExecuteFetch(artifactViewCreateSQL, 0, false); err != nil {
		return err
	}
	if err := e.clearArtifacts(ctx, onlineDDL.UUID); err != nil {
		return err
	}
	if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactViewName); err != nil {
		return err
	}

	// view created in requested format, but under different name. We now swap the views
	swapQuery, _, err := e.generateSwapTablesStatement(ctx, onlineDDL.Table, artifactViewName)
	if err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(swapQuery, 0, false); err != nil {
		return err
	}
	// Make sure this is considered as an ALTER.
	// Either the user issued a ALTER VIEW, and the action is trivially ALTER,
	// or the user issues a CREATE OR REPLACE, and the view existed, in which case this is implicitly an ALTER
	if err := e.updateDDLAction(ctx, onlineDDL.UUID, sqlparser.AlterStr); err != nil {
		return err
	}

	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)

	return nil
}

// addInstantAlgorithm adds or modifies the AlterTable's ALGORITHM to INSTANT
func (e *Executor) addInstantAlgorithm(alterTable *sqlparser.AlterTable) {
	instantOpt := sqlparser.AlgorithmValue("INSTANT")
	for i, opt := range alterTable.AlterOptions {
		if _, ok := opt.(sqlparser.AlgorithmValue); ok {
			// replace an existing algorithm
			alterTable.AlterOptions[i] = instantOpt
			return
		}
	}
	// append an algorithm
	alterTable.AlterOptions = append(alterTable.AlterOptions, instantOpt)
}

// executeSpecialAlterDDLActionMigrationIfApplicable sees if the given migration can be executed via special execution path, that isn't a full blown online schema change process.
func (e *Executor) executeSpecialAlterDDLActionMigrationIfApplicable(ctx context.Context, onlineDDL *schema.OnlineDDL) (specialMigrationExecuted bool, err error) {
	// Before we jump on to strategies... Some ALTERs can be optimized without having to run through
	// a full online schema change process. Let's find out if this is the case!
	conn, err := dbconnpool.NewDBConnection(ctx, e.env.Config().DB.DbaConnector())
	if err != nil {
		return false, err
	}
	_, err = conn.ExecuteFetch(fmt.Sprintf("use %s", onlineDDL.Schema), 1000, false)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	_, capableOf, _ := mysql.GetFlavor(conn.ServerVersion, nil)

	specialPlan, err := e.analyzeSpecialAlterPlan(ctx, onlineDDL, capableOf)
	if err != nil {
		return false, err
	}
	if specialPlan == nil {
		return false, nil
	}

	switch specialPlan.operation {
	case instantDDLSpecialOperation:
		e.addInstantAlgorithm(specialPlan.alterTable)
		onlineDDL.SQL = sqlparser.CanonicalString(specialPlan.alterTable)
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return false, err
		}
	case dropRangePartitionSpecialOperation:
		dropPartition := func() error {
			artifactTableName, err := schema.GenerateGCTableName(schema.HoldTableGCState, newGCTableRetainTime())
			if err != nil {
				return err
			}
			if err := e.updateArtifacts(ctx, onlineDDL.UUID, artifactTableName); err != nil {
				return err
			}

			// Apply CREATE TABLE for artifact table
			if _, err := e.createTableLike(ctx, artifactTableName, onlineDDL, conn); err != nil {
				return err
			}
			// Remove partitioning
			parsed := sqlparser.BuildParsedQuery(sqlAlterTableRemovePartitioning, artifactTableName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			// Exchange with partition
			partitionName := specialPlan.Detail("partition_name")
			parsed = sqlparser.BuildParsedQuery(sqlAlterTableExchangePartition, onlineDDL.Table, partitionName, artifactTableName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			// Drop table's partition
			parsed = sqlparser.BuildParsedQuery(sqlAlterTableDropPartition, onlineDDL.Table, partitionName)
			if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
				return err
			}
			return nil
		}
		if err := dropPartition(); err != nil {
			return false, err
		}
	case addRangePartitionSpecialOperation:
		if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
			return false, err
		}
	default:
		return false, nil
	}
	if err := e.updateMigrationSpecialPlan(ctx, onlineDDL.UUID, specialPlan.String()); err != nil {
		return true, err
	}
	_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
	return true, nil
}

// executeAlterDDLActionMigration
func (e *Executor) executeAlterDDLActionMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}
	ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
	if err != nil {
		return failMigration(err)
	}
	if _, isAlterView := ddlStmt.(*sqlparser.AlterView); isAlterView {
		// Same treatment for all online strategies
		exists, err := e.tableExists(ctx, onlineDDL.Schema, onlineDDL.Table)
		if err != nil {
			return failMigration(err)
		}
		if !exists {
			// We cannot ALTER VIEW if the view does not exist. We could bail out directly here,
			// but we prefer to actually get an authentic MySQL error. We know MySQL will fail running
			// this statement.
			_, err := e.executeDirectly(ctx, onlineDDL)
			return failMigration(err)
		}
		// OK, view exists
		if err := e.executeAlterViewOnline(ctx, onlineDDL); err != nil {
			return failMigration(err)
		}
		return nil
	}
	// This is a real TABLE and not a VIEW

	// Before we jump on to strategies... Some ALTERs can be optimized without having to run through
	// a full online schema change process. Let's find out if this is the case!
	specialMigrationExecuted, err := e.executeSpecialAlterDDLActionMigrationIfApplicable(ctx, onlineDDL)
	if err != nil {
		return failMigration(err)
	}
	if specialMigrationExecuted {
		return nil
	}

	// OK, nothing special about this ALTER. Let's go ahead and execute it.
	switch onlineDDL.Strategy {
	case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.ExecuteWithVReplication(ctx, onlineDDL, nil); err != nil {
				failMigration(err)
			}
		}()
	case schema.DDLStrategyMySQL:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if _, err := e.executeDirectly(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	default:
		{
			return failMigration(fmt.Errorf("Unsupported strategy: %+v", onlineDDL.Strategy))
		}
	}
	return nil
}

// executeMigration executes a single migration. It analyzes the migration type:
// - is it declarative?
// - is it CREATE / DROP / ALTER?
// - it is a Revert request?
// - what's the migration strategy?
// The function invokes the appropriate handlers for each of those cases.
func (e *Executor) executeMigration(ctx context.Context, onlineDDL *schema.OnlineDDL) error {
	defer e.triggerNextCheckInterval()
	failMigration := func(err error) error {
		return e.failMigration(ctx, onlineDDL, err)
	}

	ddlAction, err := onlineDDL.GetAction()
	if err != nil {
		return failMigration(err)
	}

	// See if this is a duplicate submission. A submission is considered duplicate if it has the exact same
	// migration context and DDL as a previous one. We are only interested in our scenario in a duplicate
	// whose predecessor is "complete". If this is the case, then we can mark our own migration as
	// implicitly "complete", too.
	{
		completedUUID, err := e.getCompletedMigrationByContextAndSQL(ctx, onlineDDL)
		if err != nil {
			return err
		}
		if completedUUID != "" {
			// Yep. We mark this migration as implicitly complete, and we're done with it!
			_ = e.onSchemaMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusComplete, false, progressPctFull, etaSecondsNow, rowsCopiedUnknown, emptyHint)
			_ = e.updateMigrationMessage(ctx, onlineDDL.UUID, fmt.Sprintf("duplicate DDL as %s for migration context %s", completedUUID, onlineDDL.MigrationContext))
			return nil
		}
	}

	switch ddlAction {
	case sqlparser.DropDDLAction:
		go func() error {
			return e.executeDropDDLActionMigration(ctx, onlineDDL)
		}()
	case sqlparser.CreateDDLAction:
		go func() error {
			return e.executeCreateDDLActionMigration(ctx, onlineDDL)
		}()
	case sqlparser.AlterDDLAction:
		return e.executeAlterDDLActionMigration(ctx, onlineDDL)
	case sqlparser.RevertDDLAction:
		go func() {
			e.migrationMutex.Lock()
			defer e.migrationMutex.Unlock()

			if err := e.executeRevert(ctx, onlineDDL); err != nil {
				failMigration(err)
			}
		}()
	}
	return nil
}

// runNextMigration picks up to one 'ready' migration that is able to run, and executes it.
// Possible scenarios:
// - no migration is in 'ready' state -- nothing to be done
// - a migration is 'ready', but conflicts with other running migrations -- try another 'ready' migration
// - multiple migrations are 'ready' -- we just handle one here
// Note that per the above breakdown, and due to potential conflicts, it is possible to have one or
// more 'ready' migration, and still none is executed.
func (e *Executor) runNextMigration(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if !e.reviewedRunningMigrationsFlag {
		// Since Open(), we havent's once executed reviewRunningMigrations() successfully.
		// This means we may not have a good picture of what is actually running. Perhaps there's
		// a vreplication migration from a pre-PRS/ERS that we still need to learn about?
		// We're going to be careful here, and avoid running new migrations until we have
		// a better picture. It will likely take a couple seconds till next iteration.
		// This delay only takes place shortly after Open().
		return nil
	}

	// if there are any online DDL tasks resume from 'paused' and its status before paused is 'running',
	// then we should run them first if they can run
	migrationsToRunContinue, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectReadyMigrationsToRunContinue)
	if err != nil {
		return err
	}
	for _, migrationToRunContinueRow := range migrationsToRunContinue.Named().Rows {
		uuidToRunContinue := migrationToRunContinueRow["migration_uuid"].ToString()
		onlineDDLToRunContinue, _, err := e.readMigration(ctx, uuidToRunContinue)
		if err != nil {
			return err
		}
		if conflictFound, _ := e.isAnyConflictingMigrationRunning(onlineDDLToRunContinue); conflictFound {
			continue // this migration conflicts with a running one
		}

		if err = e.continueVReplMigration(ctx, onlineDDLToRunContinue.Schema, onlineDDLToRunContinue.UUID); err != nil {
			return err
		}
		if err = e.clearMigrationStatusBeforePausedAndSetRunning(ctx, onlineDDLToRunContinue.UUID); err != nil {
			return err
		}
		log.Infof("Executor.runNextMigration: migration %s was paused while running and unpaused , it is non conflicting and will be executed next", onlineDDLToRunContinue.UUID)
		return nil
	}

	// getNonConflictingMigration finds a single 'ready' migration which does not conflict with running migrations.
	// Conflicts are:
	// - a migration is 'ready', but there is another 'paused' migration which status before paused is 'running' or 'ready'. Its id is smaller than the ready one, and it also has the same table with the ready one.
	// - a migration is 'ready' but is not set to run _concurrently_, and there's a running migration that is also non-concurrent
	// - a migration is 'ready' but there's another migration 'running' on the exact same table
	getNonConflictingMigration := func() (*schema.OnlineDDL, error) {
		pendingMigrationsUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
		if err != nil {
			return nil, err
		}
		r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectReadyMigrations)
		if err != nil {
			return nil, err
		}

		pausedMigrations, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectMigrationsPausedWhenReadyOrRunning)
		if err != nil {
			return nil, err
		}

		for _, row := range r.Named().Rows {
			id, _ := row["id"].ToInt64()
			table := row["mysql_table"].ToString()
			uuid := row["migration_uuid"].ToString()
			tableSchema := row["mysql_schema"].ToString()
			onlineDDL, migrationRow, err := e.readMigration(ctx, uuid)
			if err != nil {
				return nil, err
			}
			isImmediateOperation := migrationRow.AsBool("is_immediate_operation", false)

			// there is another 'paused' migration which status before paused is 'running' or 'ready'.
			// Its id is smaller than the ready one, and it also has the same table with the ready one.
			migrationReadyOrRunningBeforeIsPaused := false
			for _, pausedMigrationRow := range pausedMigrations.Named().Rows {
				pausedMigrationID, _ := pausedMigrationRow["id"].ToInt64()
				pausedMigrationTable := pausedMigrationRow["mysql_table"].ToString()
				pausedMigrationSchema := pausedMigrationRow["mysql_schema"].ToString()
				if pausedMigrationID < id && pausedMigrationTable == table && tableSchema == pausedMigrationSchema {
					migrationReadyOrRunningBeforeIsPaused = true
					break
				}
			}
			if migrationReadyOrRunningBeforeIsPaused {
				continue
			}

			if conflictFound, _ := e.isAnyConflictingMigrationRunning(onlineDDL); conflictFound {
				continue // this migration conflicts with a running one
			}
			if e.countOwnedRunningMigrations() >= maxConcurrentOnlineDDLs {
				continue // too many running migrations
			}
			if isImmediateOperation && onlineDDL.StrategySetting().IsInOrderCompletion() {
				// This migration is immediate: if we run it now, it will complete within a second or two at most.
				if len(pendingMigrationsUUIDs) > 0 && pendingMigrationsUUIDs[0] != onlineDDL.UUID {
					continue
				}
			}
			// This migration seems good to go
			return onlineDDL, err
		}
		// no non-conflicting migration found...
		// Either all ready migrations are conflicting, or there are no ready migrations...
		return nil, nil
	}

	onlineDDL, err := getNonConflictingMigration()
	if err != nil {
		return err
	}
	if onlineDDL == nil {
		// nothing to do
		return nil
	}
	{
		// We strip out any VT query comments because our simplified parser doesn't work well with comments
		ddlStmt, _, err := schema.ParseOnlineDDLStatement(onlineDDL.SQL)
		if err == nil {
			ddlStmt.SetComments(sqlparser.Comments{})
			onlineDDL.SQL = sqlparser.String(ddlStmt)
		}
	}
	log.Infof("Executor.runNextMigration: migration %s is non conflicting and will be executed next", onlineDDL.UUID)
	e.executeMigration(ctx, onlineDDL)
	return nil
}

// readVReplStream reads mysql.vreplication entries for given workflow
func (e *Executor) readVReplStream(ctx context.Context, uuid string, okIfMissing bool) (*VReplStream, error) {
	query, err := sqlparser.ParseAndBind(sqlReadVReplStream,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 && okIfMissing {
		return nil, nil
	}
	row := r.Named().Row()
	if row == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Cannot find unique workflow for UUID: %+v", uuid)
	}
	s := &VReplStream{
		id:                   row.AsInt64("id", 0),
		workflow:             row.AsString("workflow", ""),
		source:               row.AsString("source", ""),
		pos:                  row.AsString("pos", ""),
		timeUpdated:          row.AsInt64("time_updated", 0),
		timeHeartbeat:        row.AsInt64("time_heartbeat", 0),
		timeThrottled:        row.AsInt64("time_throttled", 0),
		componentThrottled:   row.AsString("component_throttled", ""),
		transactionTimestamp: row.AsInt64("transaction_timestamp", 0),
		state:                row.AsString("state", ""),
		message:              row.AsString("message", ""),
		rowsCopied:           row.AsInt64("rows_copied", 0),
		bls:                  &binlogdatapb.BinlogSource{},
	}
	if err := prototext.Unmarshal([]byte(s.source), s.bls); err != nil {
		return nil, err
	}
	return s, nil
}

// isVReplMigrationReadyToCutOver sees if the vreplication migration has completed the row copy
// and is up to date with the binlogs.
func (e *Executor) isVReplMigrationReadyToCutOver(ctx context.Context, s *VReplStream) (isReady bool, err error) {
	failpoint.Inject(failpointkey.IsVReplMigrationReadyToCutOver.Name, func(val failpoint.Value) {
		failpoint.Return(val.(bool), nil)
	})
	// Check all the cases where migration is still running:
	{
		// when ready to cut-over, pos must have some value
		if s.pos == "" {
			return false, nil
		}
	}
	{
		// Both time_updated and transaction_timestamp must be in close priximity to each
		// other and to the time now, otherwise that means we're lagging and it's not a good time
		// to cut-over
		durationDiff := func(t1, t2 time.Time) time.Duration {
			diff := t1.Sub(t2)
			if diff < 0 {
				diff = -diff
			}
			return diff
		}
		timeNow := time.Now()
		timeUpdated := time.Unix(s.timeUpdated, 0)
		if durationDiff(timeNow, timeUpdated) > vreplicationCutOverThreshold {
			return false, nil
		}
		// Let's look at transaction timestamp. This gets written by any ongoing
		// writes on the server (whether on this table or any other table)
		transactionTimestamp := time.Unix(s.transactionTimestamp, 0)
		if durationDiff(timeNow, transactionTimestamp) > vreplicationCutOverThreshold {
			return false, nil
		}
	}
	{
		// copy_state must have no entries for this vreplication id: if entries are
		// present that means copy is still in progress
		query, err := sqlparser.ParseAndBind(sqlReadCountCopyState,
			sqltypes.Int64BindVariable(s.id),
		)
		if err != nil {
			return false, err
		}
		r, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
		if err != nil {
			return false, err
		}
		csRow := r.Named().Row()
		if csRow == nil {
			return false, err
		}
		count := csRow.AsInt64("cnt", 0)
		if count > 0 {
			// Still copying
			return false, nil
		}
	}

	return true, nil
}

// isVReplMigrationRunning sees if there is a VReplication migration actively running
func (e *Executor) isVReplMigrationRunning(ctx context.Context, uuid string) (isRunning bool, s *VReplStream, err error) {
	s, err = e.readVReplStream(ctx, uuid, true)
	if err != nil {
		return false, s, err
	}
	if s == nil {
		return false, s, nil
	}
	switch s.state {
	case binlogplayer.BlpError:
		return false, s, nil
	case binlogplayer.VReplicationInit, binlogplayer.VReplicationCopying, binlogplayer.BlpRunning:
		return true, s, nil
	}
	if strings.Contains(strings.ToLower(s.message), "error") {
		return false, s, nil
	}
	return false, s, nil
}

// reviewRunningMigrations iterates migrations in 'running' state. Normally there's only one running, which was
// spawned by this tablet; but vreplication migrations could also resume from failure.
func (e *Executor) reviewRunningMigrations(ctx context.Context) (countRunnning int, cancellable []*cancellableMigration, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if atomic.LoadInt64(&e.isOpen) == 0 {
		return countRunnning, cancellable, nil
	}

	var currentUserThrottleRatio float64
	if err := e.lagThrottler.CheckIsReady(); err == nil {
		// No point in reviewing throttler info if it's not enabled&open
		for _, app := range e.lagThrottler.ThrottledApps() {
			if app.AppName == throttlerOnlineDDLApp {
				currentUserThrottleRatio = app.Ratio
				break
			}
		}
	}

	var throttlerOnce sync.Once
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectRunningMigrations)
	if err != nil {
		return countRunnning, cancellable, err
	}
	pendingMigrationsUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return countRunnning, cancellable, err
	}
	uuidsFoundRunning := map[string]bool{}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		onlineDDL, migrationRow, err := e.readMigration(ctx, uuid)
		if err != nil {
			return countRunnning, cancellable, err
		}
		postponeCompletion := row.AsBool("postpone_completion", false)
		elapsedSeconds := row.AsInt64("elapsed_seconds", 0)

		if stowawayTable := row.AsString("stowaway_table", ""); stowawayTable != "" {
			// whoa
			// stowawayTable is an original table stowed away while cutting over a vrepl migration, see call to cutOverVReplMigration() down below in this function.
			// In a normal operation, the table should not exist outside the scope of cutOverVReplMigration
			// If it exists, that means a tablet crashed while running a cut-over, and left the database in a bad state, where the migrated table does not exist.
			// thankfully, we have tracked this situation and just realized what happened. Now, first thing to do is to restore the original table.
			log.Infof("found stowaway table %s journal in migration %s for table %s", stowawayTable, uuid, onlineDDL.Table)
			attemptMade, err := e.renameTableIfApplicable(ctx, onlineDDL.Schema, stowawayTable, onlineDDL.Table)
			if err != nil {
				// unable to restore table; we bail out, and we will try again next round.
				return countRunnning, cancellable, err
			}
			// success
			if attemptMade {
				log.Infof("stowaway table %s restored back into %s", stowawayTable, onlineDDL.Table)
			} else {
				log.Infof("stowaway table %s did not exist and there was no need to restore it", stowawayTable)
			}
			// OK good, table restored. We can remove the record.
			if err := e.updateMigrationStowawayTable(ctx, uuid, ""); err != nil {
				return countRunnning, cancellable, err
			}
		}

		uuidsFoundRunning[uuid] = true

		_ = e.updateMigrationUserThrottleRatio(ctx, uuid, currentUserThrottleRatio)
		switch onlineDDL.StrategySetting().Strategy {
		case schema.DDLStrategyOnline, schema.DDLStrategyVitess:
			{
				// We check the mysql.vreplication table
				s, err := e.readVReplStream(ctx, uuid, true)
				if err != nil {
					return countRunnning, cancellable, err
				}
				isVreplicationTestSuite := onlineDDL.StrategySetting().IsVreplicationTestSuite()
				if isVreplicationTestSuite {
					e.triggerNextCheckInterval()
				}
				if s == nil {
					continue
				}
				// Let's see if vreplication indicates an error. Many errors are recoverable, and
				// we do not wish to fail on first sight. We will use LastError to repeatedly
				// check if this error persists, until finally, after some timeout, we give up.
				if _, ok := e.vreplicationLastError[uuid]; !ok {
					e.vreplicationLastError[uuid] = vterrors.NewLastError(
						fmt.Sprintf("Online DDL migration %v", uuid),
						staleMigrationMinutes*time.Minute,
					)
				}
				lastError := e.vreplicationLastError[uuid]
				isTerminal, vreplError := s.hasError()
				lastError.Record(vreplError)
				if isTerminal || !lastError.ShouldRetry() {
					cancellable = append(cancellable, newCancellableMigration(uuid, s.message))
				}
				if s.isRunning() {
					// This VRepl migration may have started from outside this tablet, so
					// this executor may not own the migration _yet_. We make sure to own it.
					// VReplication migrations are unique in this respect: we are able to complete
					// a vreplicaiton migration started by another tablet.
					e.ownedRunningMigrations.Store(uuid, onlineDDL)
					if lastVitessLivenessIndicator := migrationRow.AsInt64("vitess_liveness_indicator", 0); lastVitessLivenessIndicator < s.livenessTimeIndicator() {
						_ = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
						_ = e.updateVitessLivenessIndicator(ctx, uuid, s.livenessTimeIndicator())
					}
					if onlineDDL.TabletAlias != e.TabletAliasString() {
						_ = e.updateMigrationTablet(ctx, uuid)
						log.Infof("migration %s adopted by tablet %s", uuid, e.TabletAliasString())
					}
					_ = e.updateRowsCopied(ctx, uuid, s.rowsCopied)
					_ = e.updateMigrationProgressByRowsCopied(ctx, uuid, s.rowsCopied)
					_ = e.updateMigrationETASecondsByProgress(ctx, uuid)
					_ = e.updateMigrationLastThrottled(ctx, uuid, s.timeThrottled, s.componentThrottled)

					isReady, err := e.isVReplMigrationReadyToCutOver(ctx, s)
					if err != nil {
						_ = e.updateMigrationMessage(ctx, uuid, fmt.Sprintf("isVReplMigrationReadyToCutOver failed: err=%v", err))
						return countRunnning, cancellable, err
					}
					if isReady && isVreplicationTestSuite {
						// This is a endtoend test suite execution. We intentionally delay it by at least
						// vreplicationTestSuiteWaitSeconds
						if elapsedSeconds < vreplicationTestSuiteWaitSeconds {
							isReady = false
						}
					}
					// Indicate to outside observers whether the migration is generally ready to complete.
					// In the case of a postponed migration, we will not complete it, but the user will
					// understand whether "now is a good time" or "not there yet"
					_ = e.updateMigrationReadyToComplete(ctx, uuid, isReady)
					if postponeCompletion {
						// override. Even if migration is ready, we do not complete it.
						isReady = false
					}
					if isReady && onlineDDL.StrategySetting().IsInOrderCompletion() {
						if len(pendingMigrationsUUIDs) > 0 && pendingMigrationsUUIDs[0] != onlineDDL.UUID {
							// wait for earlier pending migrations to complete
							isReady = false
						}
					}
					if isReady {
						if err := e.cutOverVReplMigration(ctx, s); err != nil {
							_ = e.updateMigrationMessage(ctx, uuid, fmt.Sprintf("cutOverVReplMigration failed: err=%v", err))
							log.Errorf("cutOverVReplMigration failed: err=%v", err)
							if merr, ok := err.(*mysql.SQLError); ok {
								switch merr.Num {
								case mysql.ERTooLongIdent:
									go e.CancelMigration(ctx, uuid, err.Error(), false)
								}
							}
							return countRunnning, cancellable, err
						}
					}
					go throttlerOnce.Do(func() {
						if e.lagThrottler.CheckIsReady() != nil {
							return
						}
						// Self healing: in the following scenario:
						// - a vitess migration
						// - with on demand heartbeats
						// - the streamer running on a replica
						// - the streamer was throttled for long enough
						// - then vplayer and vcopier are locked, waiting for the streamer to do something
						// - since they are blocked, they're not running throttler checks
						// - since streamer runs on replica, it only checks that replica
						// - therefore no one asking for on-demand heartbeats
						// - then, if the conditions for the streamer's throttling are done, the streamer then thinks there's replication lag, with nothing to remediate it.
						// - it's a deadlock.
						// And so, once per reviewRunningMigrations(), and assuming there _are_ running migrations, we ensure to hit a throttler check. This will kick
						// on-demand heartbeats, unlocking the deadlock.
						e.lagThrottler.CheckByType(ctx, throttlerOnlineDDLApp, "", throttleCheckFlags, throttle.ThrottleCheckPrimaryWrite)
					})
				}
			}
		}
		countRunnning++
	}
	{
		// now, let's look at UUIDs we own and _think_ should be running, and see which of tham _isn't_ actually running or pending...
		uuidsFoundPending := map[string]bool{}
		for _, uuid := range pendingMigrationsUUIDs {
			uuidsFoundPending[uuid] = true
		}

		e.ownedRunningMigrations.Range(func(k, _ any) bool {
			uuid, ok := k.(string)
			if !ok {
				return true
			}
			// due to race condition, it's possible that ownedRunningMigrations will list a migration
			// that is _just about to run_ but is still, in fact, in `ready` state. This is fine.
			// If we find such a migration, we do nothing. We're only looking for migrations we really
			// don't have any information of.
			if !uuidsFoundRunning[uuid] && !uuidsFoundPending[uuid] {
				log.Infof("removing migration %s from ownedRunningMigrations because it's not running and not pending", uuid)
				e.ownedRunningMigrations.Delete(uuid)
			}
			return true
		})
	}

	e.reviewedRunningMigrationsFlag = true
	return countRunnning, cancellable, nil
}

// reviewStaleMigrations marks as 'failed' migrations whose status is 'running' but which have
// shown no liveness in past X minutes. It also attempts to terminate them
func (e *Executor) reviewStaleMigrations(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlSelectStaleMigrations,
		sqltypes.Int64BindVariable(staleMigrationMinutes),
	)
	if err != nil {
		return err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()

		onlineDDL, _, err := e.readMigration(ctx, uuid)
		if err != nil {
			return err
		}
		log.Infof("reviewStaleMigrations: stale migration found: %s", onlineDDL.UUID)
		message := fmt.Sprintf("stale migration %s: found running but indicates no liveness in the past %v minutes", onlineDDL.UUID, staleMigrationMinutes)
		if onlineDDL.TabletAlias != e.TabletAliasString() {
			// This means another tablet started the migration, and the migration has failed due to the tablet failure (e.g. primary failover)
			if err := e.updateTabletFailure(ctx, onlineDDL.UUID); err != nil {
				return err
			}
			message = fmt.Sprintf("%s; executed by different tablet %s", message, onlineDDL.TabletAlias)
		}
		if _, err := e.terminateMigration(ctx, onlineDDL); err != nil {
			message = fmt.Sprintf("error terminating migration (%v): %v", message, err)
			e.updateMigrationMessage(ctx, onlineDDL.UUID, message)
			continue // we still want to handle rest of migrations
		}
		if err := e.updateMigrationMessage(ctx, onlineDDL.UUID, message); err != nil {
			return err
		}
		if err := e.updateMigrationStatus(ctx, onlineDDL.UUID, schema.OnlineDDLStatusFailed); err != nil {
			return err
		}
		defer e.triggerNextCheckInterval()
		_ = e.updateMigrationStartedTimestamp(ctx, uuid)
		// Because the migration is stale, it may not update completed_timestamp. It is essential to set completed_timestamp
		// as this is then used when cleaning artifacts
		if err := e.updateMigrationTimestamp(ctx, "completed_timestamp", onlineDDL.UUID); err != nil {
			return err
		}
	}

	return nil
}

// retryTabletFailureMigrations looks for migrations failed by tablet failure (e.g. by failover)
// and retry them (put them back in the queue)
func (e *Executor) retryTabletFailureMigrations(ctx context.Context) error {
	_, err := e.retryMigrationWhere(ctx, sqlWhereTabletFailure)
	return err
}

func (e *Executor) tabletManagerClient() tmclient.TabletManagerClient {
	return tmclient.NewTabletManagerClient()
}

// vreplicationExec runs a vreplication query, and makes sure to initialize vreplication
func (e *Executor) vreplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	return tmClient.VReplicationExec(ctx, tablet, query)
}

// reloadSchema issues a ReloadSchema on this tablet
func (e *Executor) reloadSchema(ctx context.Context) error {
	tmClient := e.tabletManagerClient()
	defer tmClient.Close()

	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}
	return tmClient.ReloadSchema(ctx, tablet.Tablet, "")
}

// deleteVReplicationEntry cleans up a mysql.vreplication entry; this function is called as part of
// migration termination and as part of artifact cleanup
func (e *Executor) deleteVReplicationEntry(ctx context.Context, tableSchema string, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlDeleteVReplStream,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	tablet, err := e.ts.GetTablet(ctx, e.tabletAlias)
	if err != nil {
		return err
	}

	if _, err := e.vreplicationExec(ctx, tablet.Tablet, query); err != nil {
		return err
	}
	return nil
}

// gcArtifactTable garbage-collects a single table
func (e *Executor) gcArtifactTable(ctx context.Context, tableSchema, artifactTable, uuid string, t time.Time) (string, error) {
	tableExists, err := e.tableExists(ctx, tableSchema, artifactTable)
	if err != nil {
		return "", err
	}
	if !tableExists {
		return "", nil
	}
	// We've already concluded in gcArtifacts() that this table was held for long enough.
	// We therefore move it into PURGE state.
	renameStatement, toTableName, err := schema.GenerateRenameStatementWithUUID(tableSchema, artifactTable, schema.PurgeTableGCState, schema.OnlineDDLToGCUUID(uuid), t)
	if err != nil {
		return toTableName, err
	}
	_, err = e.execQuery(ctx, tableSchema, renameStatement)
	return toTableName, err
}

// gcArtifacts garbage-collects migration artifacts from completed/failed migrations
func (e *Executor) gcArtifacts(ctx context.Context) error {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	if _, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlFixCompletedTimestamp); err != nil {
		// This query fixes a bug where stale migrations were marked as 'failed' without updating 'completed_timestamp'
		// see https://github.com/vitessio/vitess/issues/8499
		// Running this query retroactively sets completed_timestamp
		// This 'if' clause can be removed in version v13
		return err
	}
	query, err := sqlparser.ParseAndBind(sqlSelectUncollectedArtifacts,
		sqltypes.Int64BindVariable(int64((retainOnlineDDLTables).Seconds())),
	)
	if err != nil {
		return err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return err
	}
	for _, row := range r.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		artifacts := row["artifacts"].ToString()
		logPath := row["log_path"].ToString()
		tableSchema := row["mysql_schema"].ToString()

		log.Infof("Executor.gcArtifacts: will GC artifacts for migration %s", uuid)
		// Remove tables:
		artifactTables := textutil.SplitDelimitedList(artifacts)

		timeNow := time.Now()
		for i, artifactTable := range artifactTables {
			// We wish to generate distinct timestamp values for each table in this UUID,
			// because all tables will be renamed as _something_UUID_timestamp. Since UUID
			// is shared for all artifacts in this loop, we differentiate via timestamp
			log.Infof("Executor.gcArtifacts: will GC artifact %s for migration %s", artifactTable, uuid)
			t := timeNow.Add(time.Duration(i) * time.Second).UTC()
			toTableName, err := e.gcArtifactTable(ctx, tableSchema, artifactTable, uuid, t)
			if err != nil {
				return vterrors.Wrapf(err, "in gcArtifacts() for %s", artifactTable)
			}
			log.Infof("Executor.gcArtifacts: renamed away artifact %s to %s", artifactTable, toTableName)
		}

		// Remove logs:
		{
			// logPath is in 'hostname:/path/to/logs' format
			tokens := strings.SplitN(logPath, ":", 2)
			logPath = tokens[len(tokens)-1]
			if err := os.RemoveAll(logPath); err != nil {
				return err
			}
		}

		// while the next function only applies to 'online' strategy ALTER and REVERT, there is no
		// harm in invoking it for other migrations.
		if err := e.deleteVReplicationEntry(ctx, tableSchema, uuid); err != nil {
			return err
		}

		if err := e.updateMigrationTimestamp(ctx, "cleanup_timestamp", uuid); err != nil {
			return err
		}
		log.Infof("Executor.gcArtifacts: done migration %s", uuid)
	}

	return nil
}

// onMigrationCheckTick runs all migrations life cycle
func (e *Executor) onMigrationCheckTick() {
	// This function can be called by multiple triggers. First, there's the normal ticker.
	// Then, any time a migration completes, we set a timer to trigger this function.
	// also, any time a new INSERT arrives, we set a timer to trigger this function.
	// Some of these may be correlated. To avoid spamming of this function we:
	// - ensure the function is non-reentrant, using tickReentranceFlag
	// - clean up tickReentranceFlag 1 second after function completes; this throttles calls to
	//   this function at no more than 1/sec rate.
	if atomic.CompareAndSwapInt64(&e.tickReentranceFlag, 0, 1) {
		defer time.AfterFunc(time.Second, func() { atomic.StoreInt64(&e.tickReentranceFlag, 0) })
	} else {
		// An instance of this function is already running
		return
	}

	if e.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
		return
	}

	ctx := context.Background()
	if err := e.retryTabletFailureMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.reviewQueuedMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.scheduleNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if err := e.runNextMigration(ctx); err != nil {
		log.Error(err)
	}
	if _, cancellable, err := e.reviewRunningMigrations(ctx); err != nil {
		log.Error(err)
	} else if err := e.cancelMigrations(ctx, cancellable, false); err != nil {
		log.Error(err)
	}
	if err := e.reviewStaleMigrations(ctx); err != nil {
		log.Error(err)
	}
	if err := e.gcArtifacts(ctx); err != nil {
		log.Error(err)
	}
}

func (e *Executor) updateMigrationStartedTimestamp(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationStartedTimestamp,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	if err != nil {
		log.Errorf("FAIL updateMigrationStartedTimestamp: uuid=%s, error=%v", uuid, err)
	}
	return err
}

func (e *Executor) updateMigrationTimestamp(ctx context.Context, timestampColumn string, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateMigrationTimestamp, timestampColumn,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	if err != nil {
		log.Errorf("FAIL updateMigrationStartedTimestamp: uuid=%s, timestampColumn=%v, error=%v", uuid, timestampColumn, err)
	}
	return err
}

func (e *Executor) updateMigrationLogPath(ctx context.Context, uuid string, hostname, logPath string) error {
	logFile := path.Join(logPath, migrationLogFileName)
	hostLogPath := fmt.Sprintf("%s:%s", hostname, logPath)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationLogPath,
		sqltypes.StringBindVariable(hostLogPath),
		sqltypes.StringBindVariable(logFile),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateArtifacts(ctx context.Context, uuid string, artifacts ...string) error {
	bindArtifacts := strings.Join(artifacts, ",")
	query, err := sqlparser.ParseAndBind(sqlUpdateArtifacts,
		sqltypes.StringBindVariable(bindArtifacts),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) clearArtifacts(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlClearArtifacts,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationSpecialPlan(ctx context.Context, uuid string, specialPlan string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateSpecialPlan,
		sqltypes.StringBindVariable(specialPlan),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationStage(ctx context.Context, uuid string, stage string, args ...interface{}) error {
	msg := fmt.Sprintf(stage, args...)
	log.Infof("updateMigrationStage: uuid=%s, stage=%s", uuid, msg)
	query, err := sqlparser.ParseAndBind(sqlUpdateStage,
		sqltypes.StringBindVariable(msg),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) incrementCutoverAttempts(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlIncrementCutoverAttempts,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

// updateMigrationTablet sets 'tablet' column to be this executor's tablet alias for given migration
func (e *Executor) updateMigrationTablet(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateTablet,
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

// updateTabletFailure marks a given migration as "tablet_failed"
func (e *Executor) updateTabletFailure(ctx context.Context, uuid string) error {
	parsed := sqlparser.BuildParsedQuery(sqlUpdateTabletFailure,
		":migration_uuid",
	)
	bindVars := map[string]*querypb.BindVariable{
		"migration_uuid": sqltypes.StringBindVariable(uuid),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	return err
}

func (e *Executor) updateMigrationStatusFailedOrCancelled(ctx context.Context, uuid string) error {
	log.Infof("updateMigrationStatus: transitioning migration: %s into status failed or cancelled", uuid)
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatusFailedOrCancelled,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationStatus(ctx context.Context, uuid string, status schema.OnlineDDLStatus) error {
	log.Infof("updateMigrationStatus: transitioning migration: %s into status: %s", uuid, string(status))
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStatus,
		sqltypes.StringBindVariable(string(status)),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		log.Errorf("FAIL updateMigrationStatus: uuid=%s, query=%v, error=%v", uuid, query, err)
	}
	return err
}

func (e *Executor) updateDDLAction(ctx context.Context, uuid string, actionStr string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateDDLAction,
		sqltypes.StringBindVariable(actionStr),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationMessage(ctx context.Context, uuid string, message string) error {
	log.Infof("updateMigrationMessage: uuid=%s, message=%s", uuid, message)

	maxlen := 16383
	update := func(message string) error {
		if len(message) > maxlen {
			message = message[0:maxlen]
		}
		message = strings.ToValidUTF8(message, "�")
		query, err := sqlparser.ParseAndBind(sqlUpdateMessage,
			sqltypes.StringBindVariable(message),
			sqltypes.StringBindVariable(uuid),
		)
		if err != nil {
			return err
		}
		_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
		return err
	}
	err := update(message)
	if err != nil {
		// If, for some reason, we're unable to update the error message, let's write a generic message
		err = update("unable to update with original migration error message")
	}
	return err
}

func (e *Executor) updateSchemaAnalysis(ctx context.Context, uuid string,
	addedUniqueKeys, removedUnqiueKeys int, removedUniqueKeyNames string,
	droppedNoDefaultColumnNames string, expandedColumnNames string,
	revertibleNotes string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateSchemaAnalysis,
		sqltypes.Int64BindVariable(int64(addedUniqueKeys)),
		sqltypes.Int64BindVariable(int64(removedUnqiueKeys)),
		sqltypes.StringBindVariable(removedUniqueKeyNames),
		sqltypes.StringBindVariable(droppedNoDefaultColumnNames),
		sqltypes.StringBindVariable(expandedColumnNames),
		sqltypes.StringBindVariable(revertibleNotes),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMySQLTable(ctx context.Context, uuid string, tableName string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMySQLTable,
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationETASeconds(ctx context.Context, uuid string, etaSeconds int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationETASeconds,
		sqltypes.Int64BindVariable(etaSeconds),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationProgress(ctx context.Context, uuid string, progress float64) error {
	if progress <= 0 {
		// progress starts at 0, and can only increase.
		// A value of "0" either means "This is the actual current progress" or "No information"
		// In both cases there's nothing to update
		return nil
	}
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationProgress,
		sqltypes.Float64BindVariable(progress),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationProgressByRowsCopied(ctx context.Context, uuid string, rowsCopied int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationProgressByRowsCopied,
		sqltypes.Int64BindVariable(rowsCopied),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationETASecondsByProgress(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationETASecondsByProgress,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationLastThrottled(ctx context.Context, uuid string, lastThrottledUnixTime int64, throttledCompnent string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateLastThrottled,
		sqltypes.Int64BindVariable(lastThrottledUnixTime),
		sqltypes.StringBindVariable(throttledCompnent),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationTableRows(ctx context.Context, uuid string, tableRows int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationTableRows,
		sqltypes.Int64BindVariable(tableRows),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateRowsCopied(ctx context.Context, uuid string, rowsCopied int64) error {
	if rowsCopied <= 0 {
		// Number of rows can only be positive. Zero or negative must mean "no information" and
		// we don't update the table value.
		return nil
	}
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationRowsCopied,
		sqltypes.Int64BindVariable(rowsCopied),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateVitessLivenessIndicator(ctx context.Context, uuid string, livenessIndicator int64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationVitessLivenessIndicator,
		sqltypes.Int64BindVariable(livenessIndicator),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationIsView(ctx context.Context, uuid string, isView bool) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationIsView,
		sqltypes.BoolBindVariable(isView),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationSetImmediateOperation(ctx context.Context, uuid string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationSetImmediateOperation,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationReadyToComplete(ctx context.Context, uuid string, isReady bool) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationReadyToComplete,
		sqltypes.BoolBindVariable(isReady),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	if _, err := e.execQuery(ctx, sidecardb.SidecarDBName, query); err != nil {
		return err
	}
	if val, ok := e.ownedRunningMigrations.Load(uuid); ok {
		if runningMigration, ok := val.(*schema.OnlineDDL); ok {
			var storeValue int64
			if isReady {
				storeValue = 1
			}
			atomic.StoreInt64(&runningMigration.ReadyToComplete, storeValue)
		}
	}
	return nil
}

func (e *Executor) updateMigrationStowawayTable(ctx context.Context, uuid string, tableName string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationStowawayTable,
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

func (e *Executor) updateMigrationUserThrottleRatio(ctx context.Context, uuid string, ratio float64) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateMigrationUserThrottleRatio,
		sqltypes.Float64BindVariable(ratio),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return err
	}
	_, err = e.execQuery(ctx, sidecardb.SidecarDBName, query)
	return err
}

// retryMigrationWhere retries a migration based on a given WHERE clause
func (e *Executor) retryMigrationWhere(ctx context.Context, whereExpr string) (result *sqltypes.Result, err error) {
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()
	parsed := sqlparser.BuildParsedQuery(sqlRetryMigrationWhere, ":tablet", whereExpr)
	bindVars := map[string]*querypb.BindVariable{
		"tablet": sqltypes.StringBindVariable(e.TabletAliasString()),
	}
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	result, err = e.execQuery(ctx, sidecardb.SidecarDBName, bound)
	return result, err
}

// RetryMigration marks given migration for retry
func (e *Executor) RetryMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in RETRY: %s", uuid)
	}
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlRetryMigration,
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	return e.execQuery(ctx, sidecardb.SidecarDBName, query)
}

// CleanupMigration sets migration is ready for artifact cleanup. Artifacts are not immediately deleted:
// all we do is set retain_artifacts_seconds to a very small number (it's actually a negative) so that the
// next iteration of gcArtifacts() picks up the migration's artifacts and schedules them for deletion
func (e *Executor) CleanupMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in CLEANUP: %s", uuid)
	}
	log.Infof("CleanupMigration: request to cleanup migration %s", uuid)
	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateReadyForCleanup,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	rs, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return nil, err
	}
	log.Infof("CleanupMigration: migration %s marked as ready to clean up", uuid)
	return rs, nil
}

func (e *Executor) OnDropSchema(ctx context.Context, schemaName string) (err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if schemaName == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "schema name cannot be empty")
	}
	uuidList, err := e.readMigrationsBySchemaName(ctx, schemaName)
	if err != nil {
		return err
	}
	for _, uuid := range uuidList {
		_, err := e.CancelMigration(ctx, uuid, "cancel by dropping database", false)
		if err != nil {
			return err
		}
		e.deleteMigration(ctx, uuid)
	}
	return err
}

func (e *Executor) readMigrationsBySchemaName(ctx context.Context, schemaName string) ([]string, error) {
	query, err := sqlparser.ParseAndBind(sqlSelectMigrationsBySchemaName,
		sqltypes.StringBindVariable(schemaName),
	)
	if err != nil {
		return nil, err
	}
	rs, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return nil, err
	}
	uuidResult := make([]string, 0, len(rs.Rows))
	for _, row := range rs.Named().Rows {
		uuid := row["migration_uuid"].ToString()
		uuidResult = append(uuidResult, uuid)
	}
	return uuidResult, nil
}

// CompleteMigration clears the postpone_completion flag for a given migration, assuming it was set in the first place
func (e *Executor) CompleteMigration(ctx context.Context, uuid string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in COMPLETE: %s", uuid)
	}
	log.Infof("CompleteMigration: request to complete migration %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateCompleteMigration,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	rs, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return nil, err
	}
	log.Infof("CompleteMigration: migration %s marked as unpostponed", uuid)
	return rs, nil
}

// CompletePendingMigrations completes all pending migrations (that are expected to run or are running)
// for this keyspace
func (e *Executor) CompletePendingMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	log.Infof("CompletePendingMigrations: iterating %v migrations %s", len(uuids))

	result = &sqltypes.Result{}
	for _, uuid := range uuids {
		log.Infof("CompletePendingMigrations: completing %s", uuid)
		res, err := e.CompleteMigration(ctx, uuid)
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("CompletePendingMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

// LaunchMigration clears the postpone_launch flag for a given migration, assuming it was set in the first place
func (e *Executor) LaunchMigration(ctx context.Context, uuid string, shardsArg string) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	if !schema.IsOnlineDDLUUID(uuid) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Not a valid migration ID in EXECUTE: %s", uuid)
	}
	if !e.matchesShards(shardsArg) {
		// Does not apply  to this shard!
		return &sqltypes.Result{}, nil
	}
	log.Infof("LaunchMigration: request to execute migration %s", uuid)

	e.migrationMutex.Lock()
	defer e.migrationMutex.Unlock()

	query, err := sqlparser.ParseAndBind(sqlUpdateLaunchMigration,
		sqltypes.StringBindVariable(uuid),
	)
	if err != nil {
		return nil, err
	}
	defer e.triggerNextCheckInterval()
	rs, err := e.execQuery(ctx, sidecardb.SidecarDBName, query)
	if err != nil {
		return nil, err
	}
	log.Infof("LaunchMigration: migration %s marked as unpostponed", uuid)
	return rs, nil
}

// LaunchMigrations launches all launch-postponed queued migrations for this keyspace
func (e *Executor) LaunchMigrations(ctx context.Context) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	uuids, err := e.readPendingMigrationsUUIDs(ctx)
	if err != nil {
		return result, err
	}
	r, err := e.execQuery(ctx, sidecardb.SidecarDBName, sqlSelectQueuedMigrations)
	if err != nil {
		return result, err
	}
	rows := r.Named().Rows
	log.Infof("LaunchMigrations: iterating %v migrations %s", len(rows))
	result = &sqltypes.Result{}
	for _, row := range rows {
		uuid := row["migration_uuid"].ToString()
		log.Infof("LaunchMigrations: unpostponing %s", uuid)
		res, err := e.LaunchMigration(ctx, uuid, "")
		if err != nil {
			return result, err
		}
		result.AppendResult(res)
	}
	log.Infof("LaunchMigrations: done iterating %v migrations %s", len(uuids))
	return result, nil
}

func (e *Executor) submittedMigrationConflictsWithPendingMigrationInSingletonContext(
	ctx context.Context, submittedMigration, pendingOnlineDDL *schema.OnlineDDL,
) bool {
	if pendingOnlineDDL.MigrationContext == submittedMigration.MigrationContext {
		// same migration context. this is obviously allowed
		return false
	}
	// Let's see if the pending migration is a revert:
	if _, err := pendingOnlineDDL.GetRevertUUID(); err != nil {
		// Not a revert. So the pending migration definitely conflicts with our migration.
		return true
	}

	// The pending migration is a revert
	if !pendingOnlineDDL.StrategySetting().IsSingletonContext() {
		// Aha! So, our "conflict" is with a REVERT migration, which does _not_ have a -singleton-context
		// flag. Because we want to allow REVERT migrations to run as concurrently as possible, we allow this scenario.
		return false
	}
	return true
}

// submitCallbackIfNonConflicting is called internally by SubmitMigration, and is given a callack to execute
// if the given migration does not conflict any terms. Specifically, this function looks for singleton or
// singleton-context conflicts.
// The call back can be an insertion of a new migration, or a retry of an existing migration, or whatnot.
func (e *Executor) submitCallbackIfNonConflicting(
	ctx context.Context,
	onlineDDL *schema.OnlineDDL,
	callback func() (*sqltypes.Result, error),
) (
	result *sqltypes.Result, err error,
) {
	if !onlineDDL.StrategySetting().IsSingleton() && !onlineDDL.StrategySetting().IsSingletonContext() {
		// not a singleton. No conflict
		return callback()
	}
	// This is either singleton or singleton-context

	// This entire next logic is wrapped in an anonymous func just to get the migrationMutex released
	// before calling the callback function. Reason is: the callback function itself may need to acquire
	// the mutex. And specifically, one of the callback functions used is e.RetryMigration(), which does
	// lock the mutex...
	err = func() error {
		e.migrationMutex.Lock()
		defer e.migrationMutex.Unlock()

		pendingUUIDs, err := e.readPendingMigrationsUUIDs(ctx)
		if err != nil {
			return err
		}
		switch {
		case onlineDDL.StrategySetting().IsSingleton():
			// We will reject this migration if there's any pending migration
			if len(pendingUUIDs) > 0 {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton migration rejected: found pending migrations [%s]", strings.Join(pendingUUIDs, ", "))
			}
		case onlineDDL.StrategySetting().IsSingletonContext():
			// We will reject this migration if there's any pending migration within a different context
			for _, pendingUUID := range pendingUUIDs {
				pendingOnlineDDL, _, err := e.readMigration(ctx, pendingUUID)
				if err != nil {
					return vterrors.Wrapf(err, "validateSingleton() migration: %s", pendingUUID)
				}
				if e.submittedMigrationConflictsWithPendingMigrationInSingletonContext(ctx, onlineDDL, pendingOnlineDDL) {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "singleton-context migration rejected: found pending migration: %s in different context: %s", pendingUUID, pendingOnlineDDL.MigrationContext)
				}
				// no conflict? continue looking for other pending migrations
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}
	// OK to go!
	return callback()
}

// SubmitMigration inserts a new migration request
func (e *Executor) SubmitMigration(
	ctx context.Context,
	stmt sqlparser.Statement,
) (*sqltypes.Result, error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}

	log.Infof("SubmitMigration: request to submit migration with statement: %0.50s...", sqlparser.CanonicalString(stmt))
	if ddlStmt, ok := stmt.(sqlparser.DDLStatement); ok {
		// This validation should have taken place on submission. However, the query may have mutated
		// during transfer, and this validation is here to catch any malformed mutation.
		if !ddlStmt.IsFullyParsed() {
			return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "error parsing statement")
		}
	}

	onlineDDL, err := schema.OnlineDDLFromCommentedStatement(stmt)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Error submitting migration %s: %v", sqlparser.String(stmt), err)
	}

	// The logic below has multiple steps. We hence protect the rest of the code with a mutex, only used by this function.
	e.submitMutex.Lock()
	defer e.submitMutex.Unlock()

	// Is there already a migration by this same UUID?
	storedMigration, _, err := e.readMigration(ctx, onlineDDL.UUID)
	if err != nil && err != ErrMigrationNotFound {
		return nil, vterrors.Wrapf(err, "while checking whether migration %s exists", onlineDDL.UUID)
	}
	if storedMigration != nil {
		log.Infof("SubmitMigration: migration %s already exists with migration_context=%s, table=%s", onlineDDL.UUID, storedMigration.MigrationContext, onlineDDL.Table)
		// A migration already exists with the same UUID. This is fine, we allow re-submitting migrations
		// with the same UUID, as we provide idempotency.
		// So we will _mostly_ ignore the request: we will not submit a new migration. However, we will do
		// these things:

		// 1. Check that the requested submmited migration macthes the existing one's migration-context, otherwise
		//    this doesn't seem right, not the idempotency we were looking for
		if storedMigration.MigrationContext != onlineDDL.MigrationContext {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "migration rejected: found migration %s with different context: %s than submmitted migration's context: %s", onlineDDL.UUID, storedMigration.MigrationContext, onlineDDL.MigrationContext)
		}
		// 2. Possibly, the existing migration is in 'failed' or 'cancelled' state, in which case this
		//    resubmission should retry the migration.
		return e.submitCallbackIfNonConflicting(
			ctx, onlineDDL,
			func() (*sqltypes.Result, error) { return e.RetryMigration(ctx, onlineDDL.UUID) },
		)
	}

	// OK, this is a new UUID

	_, actionStr, err := onlineDDL.GetActionStr()
	if err != nil {
		return nil, err
	}
	log.Infof("SubmitMigration: request to submit migration %s; action=%s, table=%s", onlineDDL.UUID, actionStr, onlineDDL.Table)

	revertedUUID, _ := onlineDDL.GetRevertUUID() // Empty value if the migration is not actually a REVERT. Safe to ignore error.
	retainArtifactsSeconds := int64((retainOnlineDDLTables).Seconds())
	_, allowConcurrentMigration := e.allowConcurrentMigration(onlineDDL)
	submitQuery, err := sqlparser.ParseAndBind(sqlInsertMigration,
		sqltypes.StringBindVariable(onlineDDL.UUID),
		sqltypes.StringBindVariable(onlineDDL.Schema),
		sqltypes.StringBindVariable(e.shard),
		sqltypes.StringBindVariable(onlineDDL.Schema),
		sqltypes.StringBindVariable(onlineDDL.Table),
		sqltypes.StringBindVariable(onlineDDL.SQL),
		sqltypes.StringBindVariable(string(onlineDDL.Strategy)),
		sqltypes.StringBindVariable(onlineDDL.Options),
		sqltypes.StringBindVariable(actionStr),
		sqltypes.StringBindVariable(onlineDDL.MigrationContext),
		sqltypes.StringBindVariable(string(schema.OnlineDDLStatusQueued)),
		sqltypes.StringBindVariable(e.TabletAliasString()),
		sqltypes.Int64BindVariable(retainArtifactsSeconds),
		sqltypes.BoolBindVariable(onlineDDL.StrategySetting().IsPostponeLaunch()),
		sqltypes.BoolBindVariable(onlineDDL.StrategySetting().IsPostponeCompletion()),
		sqltypes.BoolBindVariable(allowConcurrentMigration),
		sqltypes.StringBindVariable(revertedUUID),
		sqltypes.BoolBindVariable(onlineDDL.IsView()),
	)
	if err != nil {
		return nil, err
	}
	result, err := e.submitCallbackIfNonConflicting(
		ctx, onlineDDL,
		func() (*sqltypes.Result, error) { return e.execQuery(ctx, sidecardb.SidecarDBName, submitQuery) },
	)
	if err != nil {
		return nil, vterrors.Wrapf(err, "submitting migration %v", onlineDDL.UUID)

	}
	log.Infof("SubmitMigration: migration %s submitted", onlineDDL.UUID)

	defer e.triggerNextCheckInterval()

	return result, nil
}

// ShowMigrationLogs reads the migration log for a given migration
func (e *Executor) ShowMigrationLogs(ctx context.Context, stmt *sqlparser.ShowMigrationLogs) (result *sqltypes.Result, err error) {
	if atomic.LoadInt64(&e.isOpen) == 0 {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "online ddl is disabled")
	}
	_, row, err := e.readMigration(ctx, stmt.UUID)
	if err != nil {
		return nil, err
	}
	logFile := row["log_file"].ToString()
	if logFile == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "No log file for migration %v", stmt.UUID)
	}
	content, err := os.ReadFile(logFile)
	if err != nil {
		return nil, err
	}

	result = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "migration_log",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	result.Rows = append(result.Rows, []sqltypes.Value{
		sqltypes.NewVarChar(string(content)),
	})
	return result, nil
}

// onSchemaMigrationStatus is called when a status is set/changed for a running migration
func (e *Executor) onSchemaMigrationStatus(ctx context.Context,
	uuid string, status schema.OnlineDDLStatus, dryRun bool, progressPct float64, etaSeconds int64, rowsCopied int64, hint string) (err error) {
	if dryRun && status != schema.OnlineDDLStatusFailed {
		// We don't consider dry-run reports unless there's a failure
		return nil
	}
	switch status {
	case schema.OnlineDDLStatusReady:
		{
			err = e.updateMigrationTimestamp(ctx, "ready_timestamp", uuid)
		}
	case schema.OnlineDDLStatusRunning:
		{
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "liveness_timestamp", uuid)
		}
	case schema.OnlineDDLStatusComplete:
		{
			progressPct = progressPctFull
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "completed_timestamp", uuid)
		}
	case schema.OnlineDDLStatusFailed:
		{
			_ = e.updateMigrationStartedTimestamp(ctx, uuid)
			err = e.updateMigrationTimestamp(ctx, "completed_timestamp", uuid)
		}
	}
	if err != nil {
		return err
	}
	if err = e.updateMigrationStatus(ctx, uuid, status); err != nil {
		return err
	}
	if err = e.updateMigrationProgress(ctx, uuid, progressPct); err != nil {
		return err
	}
	if err = e.updateMigrationETASeconds(ctx, uuid, etaSeconds); err != nil {
		return err
	}
	if err := e.updateRowsCopied(ctx, uuid, rowsCopied); err != nil {
		return err
	}
	if hint == readyToCompleteHint {
		if err := e.updateMigrationReadyToComplete(ctx, uuid, true); err != nil {
			return err
		}
	}
	if !dryRun {
		switch status {
		case schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed:
			e.triggerNextCheckInterval()
		}
	}

	return nil
}

// OnSchemaMigrationStatus is called by TabletServer's API, which is invoked by a running gh-ost migration's hooks.
func (e *Executor) OnSchemaMigrationStatus(ctx context.Context,
	uuidParam, statusParam, dryrunParam, progressParam, etaParam, rowsCopiedParam, hint string) (err error) {
	status := schema.OnlineDDLStatus(statusParam)
	dryRun := (dryrunParam == "true")
	var progressPct float64
	if pct, err := strconv.ParseFloat(progressParam, 64); err == nil {
		progressPct = pct
	}
	var etaSeconds int64 = etaSecondsUnknown
	if eta, err := strconv.ParseInt(etaParam, 10, 64); err == nil {
		etaSeconds = eta
	}
	var rowsCopied int64
	if rows, err := strconv.ParseInt(rowsCopiedParam, 10, 64); err == nil {
		rowsCopied = rows
	}

	return e.onSchemaMigrationStatus(ctx, uuidParam, status, dryRun, progressPct, etaSeconds, rowsCopied, hint)
}

// VExec is called by a VExec invocation
// Implements vitess.io/vitess/go/vt/vttablet/vexec.Executor interface
func (e *Executor) VExec(ctx context.Context, vx *vexec.TabletVExec) (qr *querypb.QueryResult, err error) {
	response := func(result *sqltypes.Result, err error) (*querypb.QueryResult, error) {
		if err != nil {
			return nil, err
		}
		return sqltypes.ResultToProto3(result), nil
	}

	switch stmt := vx.Stmt.(type) {
	case *sqlparser.Delete:
		return nil, fmt.Errorf("DELETE statements not supported for this table. query=%s", vx.Query)
	case *sqlparser.Select:
		// todo onlineDDL: should understant vx
		return response(e.execQuery(ctx, sidecardb.SidecarDBName, vx.Query))
	case *sqlparser.Insert:
		match, err := sqlparser.QueryMatchesTemplates(vx.Query, vexecInsertTemplates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s", strings.Join(vexecInsertTemplates, "; "))
		}
		// Vexec naturally runs outside shard/schema context. It does not supply values for those columns.
		// We can fill them in.
		vx.ReplaceInsertColumnVal("shard", vx.ToStringVal(e.shard))
		tableSchema := stmt.Table.Qualifier.String()
		if tableSchema == "" {
			tableSchema = vx.Keyspace
		}
		vx.ReplaceInsertColumnVal("mysql_schema", vx.ToStringVal(tableSchema))
		vx.AddOrReplaceInsertColumnVal("tablet", vx.ToStringVal(e.TabletAliasString()))
		e.triggerNextCheckInterval()
		return response(e.execQuery(ctx, tableSchema, vx.Query))
	case *sqlparser.Update:
		match, err := sqlparser.QueryMatchesTemplates(vx.Query, vexecUpdateTemplates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s; query=%s", strings.Join(vexecUpdateTemplates, "; "), vx.Query)
		}
		if shard, _ := vx.ColumnStringVal(vx.WhereCols, "shard"); shard != "" {
			// shard is specified.
			if shard != e.shard {
				// specified shard is not _this_ shard. So we're skipping this UPDATE
				return sqltypes.ResultToProto3(emptyResult), nil
			}
		}
		statusVal, err := vx.ColumnStringVal(vx.UpdateCols, "migration_status")
		if err != nil {
			return nil, err
		}
		switch statusVal {
		case retryMigrationHint:
			return response(e.retryMigrationWhere(ctx, sqlparser.String(stmt.Where.Expr)))
		case completeMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.CompleteMigration(ctx, uuid))
		case cancelMigrationHint:
			uuid, err := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if err != nil {
				return nil, err
			}
			if !schema.IsOnlineDDLUUID(uuid) {
				return nil, fmt.Errorf("Not an Online DDL UUID: %s", uuid)
			}
			return response(e.CancelMigration(ctx, uuid, "cancel by user", true))
		case cancelAllMigrationHint:
			uuid, _ := vx.ColumnStringVal(vx.WhereCols, "migration_uuid")
			if uuid != "" {
				return nil, fmt.Errorf("Unexpetced UUID: %s", uuid)
			}
			return response(e.CancelPendingMigrations(ctx, "cancel-all by user", true))
		default:
			return nil, fmt.Errorf("Unexpected value for migration_status: %v. Supported values are: %s, %s",
				statusVal, retryMigrationHint, cancelMigrationHint)
		}
	default:
		return nil, fmt.Errorf("No handler for this query: %s", vx.Query)
	}
}
