/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2020 The Vitess Authors.

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

package gc

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"github.com/wesql/wescale/go/mysql"
	"github.com/wesql/wescale/go/timer"
	"github.com/wesql/wescale/go/vt/dbconnpool"
	"github.com/wesql/wescale/go/vt/log"
	"github.com/wesql/wescale/go/vt/schema"
	"github.com/wesql/wescale/go/vt/servenv"
	"github.com/wesql/wescale/go/vt/sqlparser"
	"github.com/wesql/wescale/go/vt/topo"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/connpool"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/throttle"
)

const (
	// evacHours is a hard coded, reasonable time for a table to spend in EVAC state
	evacHours        = 72
	throttlerAppName = "tablegc"
)

var (
	checkInterval           = 1 * time.Hour
	purgeReentranceInterval = 1 * time.Minute
	gcLifecycle             = "hold,purge,evac,drop"
)

func init() {
	servenv.OnParseFor("vtcombo", registerGCFlags)
	servenv.OnParseFor("vttablet", registerGCFlags)
}

func registerGCFlags(fs *pflag.FlagSet) {
	// checkInterval marks the interval between looking for tables in mysql server/schema
	fs.DurationVar(&checkInterval, "gc_check_interval", checkInterval, "Interval between garbage collection checks")
	// purgeReentranceInterval marks the interval between searching tables to purge
	fs.DurationVar(&purgeReentranceInterval, "gc_purge_check_interval", purgeReentranceInterval, "Interval between purge discovery checks")
	// gcLifecycle is the sequence of steps the table goes through in the process of getting dropped
	fs.StringVar(&gcLifecycle, "table_gc_lifecycle", gcLifecycle, "States for a DROP TABLE garbage collection cycle. Default is 'hold,purge,evac,drop', use any subset ('drop' implcitly always included)")
}

var (
	sqlPurgeTable       = "delete from `%a`.`%a` limit 50"
	sqlSelectVtTables   = `select table_schema, table_name, table_type from information_schema.tables where table_name like '\_vt\_%'`
	sqlDropTable        = "drop table if exists `%a`.`%a`"
	purgeReentranceFlag int64
)

// transitionRequest encapsulates a request to transition a table to next state
type transitionRequest struct {
	fromFullTableName schema.TableSchemaAndName
	isBaseTable       bool
	toGCState         schema.TableGCState
	uuid              string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TableGC is the main entity in the table garbage collection mechanism.
// This service "garbage collects" tables:
// - it checks for magically-named tables (e.g. _vt_EVAC_f6338b2af8af11eaa210f875a4d24e90_20200920063522)
// - it analyzes a table's state from its name
// - it applies operations on the table (namely purge for PURGE tables)
// - when due time, it transitions a table (via RENAME TABLE) to the next state
// - finally, it issues a DROP TABLE
// The sequence of steps is controlled by the command line variable --table_gc_lifecycle
type TableGC struct {
	isOpen          int64
	cancelOperation context.CancelFunc

	throttlerClient *throttle.Client

	env  tabletenv.Env
	pool *connpool.Pool
	ts   *topo.Server

	stateMutex sync.Mutex
	purgeMutex sync.Mutex

	purgingTables map[schema.TableSchemaAndName]bool
	// lifecycleStates indicates what states a GC table goes through. The user can set
	// this with --table_gc_lifecycle, such that some states can be skipped.
	lifecycleStates map[schema.TableGCState]bool
}

// Status published some status valus from the collector
type Status struct {
	Keyspace string
	Shard    string

	isPrimary bool
	IsOpen    bool

	purgingTables []string
}

// NewTableGC creates a table collector
func NewTableGC(env tabletenv.Env, ts *topo.Server, lagThrottler *throttle.Throttler) *TableGC {
	collector := &TableGC{
		throttlerClient: throttle.NewBackgroundClient(lagThrottler, throttlerAppName, throttle.ThrottleCheckPrimaryWrite),
		isOpen:          0,

		env: env,
		ts:  ts,
		pool: connpool.NewPool(env, "TableGCPool", tabletenv.ConnPoolConfig{
			Size:               2,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),

		purgingTables: map[schema.TableSchemaAndName]bool{},
	}

	return collector
}

// Open opens database pool and initializes the schema
func (collector *TableGC) Open() (err error) {
	collector.stateMutex.Lock()
	defer collector.stateMutex.Unlock()
	if collector.isOpen > 0 {
		// already open
		return nil
	}
	if !collector.env.Config().EnableTableGC {
		return nil
	}

	collector.lifecycleStates, err = schema.ParseGCLifecycle(gcLifecycle)
	if err != nil {
		return fmt.Errorf("Error parsing --table_gc_lifecycle flag: %+v", err)
	}

	log.Info("TableGC: opening")
	collector.pool.Open(collector.env.Config().DB.AllPrivsConnector(), collector.env.Config().DB.DbaConnector(), collector.env.Config().DB.AppDebugConnector())
	atomic.StoreInt64(&collector.isOpen, 1)

	conn, err := dbconnpool.NewDBConnection(context.Background(), collector.env.Config().DB.AllPrivsConnector())
	if err != nil {
		return err
	}
	defer conn.Close()
	serverSupportsFastDrops, err := conn.SupportsCapability(mysql.FastDropTableFlavorCapability)
	if err != nil {
		return err
	}
	if serverSupportsFastDrops {
		// MySQL 8.0.23 and onwards supports fast DROP TABLE operations. This means we don't have to
		// go through the purging & evac cycle: once the table has been held for long enough, we can just
		// move on to dropping it. Dropping a large table in 8.0.23 is expected to take several seconds, and
		// should not block other queries or place any locks on the buffer pool.
		delete(collector.lifecycleStates, schema.PurgeTableGCState)
		delete(collector.lifecycleStates, schema.EvacTableGCState)
	}

	ctx := context.Background()
	ctx, collector.cancelOperation = context.WithCancel(ctx)
	go collector.operate(ctx)

	return nil
}

// Close frees resources
func (collector *TableGC) Close() {
	log.Infof("TableGC - started execution of Close. Acquiring initMutex lock")
	collector.stateMutex.Lock()
	defer collector.stateMutex.Unlock()
	log.Infof("TableGC - acquired lock")
	if collector.isOpen == 0 {
		log.Infof("TableGC - no collector is open")
		// not open
		return
	}

	log.Info("TableGC: closing")
	if collector.cancelOperation != nil {
		collector.cancelOperation()
	}
	log.Infof("TableGC - closing pool")
	collector.pool.Close()
	atomic.StoreInt64(&collector.isOpen, 0)
	log.Infof("TableGC - finished execution of Close")
}

// operate is the main entry point for the table garbage collector operation and logic.
func (collector *TableGC) operate(ctx context.Context) {

	dropTablesChan := make(chan schema.TableSchemaAndName)
	purgeRequestsChan := make(chan bool)
	transitionRequestsChan := make(chan *transitionRequest)

	tickers := [](*timer.SuspendableTicker){}
	addTicker := func(d time.Duration) *timer.SuspendableTicker {
		t := timer.NewSuspendableTicker(d, false)
		tickers = append(tickers, t)
		return t
	}
	tableCheckTicker := addTicker(checkInterval)
	purgeReentranceTicker := addTicker(purgeReentranceInterval)

	for _, t := range tickers {
		defer t.Stop()
		// since we just started the tickers now, speed up the ticks by forcing an immediate tick
		go t.TickNow()
	}

	log.Info("TableGC: operating")
	for {
		select {
		case <-ctx.Done():
			log.Info("TableGC: done operating")
			return
		case <-tableCheckTicker.C:
			{
				log.Info("TableGC: tableCheckTicker")
				_ = collector.checkTables(ctx, dropTablesChan, transitionRequestsChan)
			}
		case <-purgeReentranceTicker.C:
			{
				// relay the request
				go func() { purgeRequestsChan <- true }()
			}
		case <-purgeRequestsChan:
			{
				log.Info("TableGC: purgeRequestsChan")
				go func() {
					fullTblName, err := collector.purge(ctx)
					if err != nil {
						log.Errorf("TableGC: error purging table %s: %+v", fullTblName.String(), err)
						return
					}
					if fullTblName.IsEmpty() {
						// No table purged (or at least not to completion)
						return
					}
					// a table was successfully purged to complection. Chances are, there's more
					// tables waiting to be purged. Let's speed things by
					// requesting another purge, instead of waiting a full hour
					// The table is now empty!
					// we happen to know at this time that the table is in PURGE state,
					// I mean, that's why we're here. We can hard code that.
					_, _, uuid, _, _ := schema.AnalyzeGCTableName(fullTblName.GetTableName())
					collector.submitTransitionRequest(ctx, transitionRequestsChan, schema.PurgeTableGCState, fullTblName, true, uuid)
					collector.removePurgingTable(fullTblName)
					// finished with this table. Maybe more tables are looking to be purged.
					// Trigger another call to purge(), instead of waiting a full purgeReentranceInterval cycle

					time.AfterFunc(time.Second, func() { purgeRequestsChan <- true })

				}()
			}
		case dropTableName := <-dropTablesChan:
			{
				log.Info("TableGC: dropTablesChan")
				if err := collector.dropTable(ctx, dropTableName); err != nil {
					log.Errorf("TableGC: error dropping table %s: %+v", dropTableName, err)
				}
			}
		case transition := <-transitionRequestsChan:
			{
				log.Info("TableGC: transitionRequestsChan")
				if err := collector.transitionTable(ctx, transition); err != nil {
					log.Errorf("TableGC: error transitioning table %s to %+v: %+v", transition.fromFullTableName.String(), transition.toGCState, err)
				}
			}
		}
	}
}

// nextState evaluates what the next state should be, given a state; this takes into account
// lifecycleStates (as generated by user supplied --table_gc_lifecycle flag)
func (collector *TableGC) nextState(fromState schema.TableGCState) *schema.TableGCState {
	var state schema.TableGCState
	switch fromState {
	case schema.HoldTableGCState:
		state = schema.PurgeTableGCState
	case schema.PurgeTableGCState:
		state = schema.EvacTableGCState
	case schema.EvacTableGCState:
		state = schema.DropTableGCState
	case schema.DropTableGCState:
		return nil
	default:
		return nil
	}
	if _, ok := collector.lifecycleStates[state]; !ok {
		return collector.nextState(state)
	}
	return &state
}

// generateTansition creates a transition request, based on current state and taking configured lifecycleStates
// into consideration (we may skip some states)
func (collector *TableGC) generateTansition(ctx context.Context, fromState schema.TableGCState, fromFullTableName schema.TableSchemaAndName, isBaseTable bool, uuid string) *transitionRequest {
	nextState := collector.nextState(fromState)
	if nextState == nil {
		return nil
	}
	return &transitionRequest{
		fromFullTableName: fromFullTableName,
		isBaseTable:       isBaseTable,
		toGCState:         *nextState,
		uuid:              uuid,
	}
}

// submitTransitionRequest generates and queues a transition request for a given table
func (collector *TableGC) submitTransitionRequest(ctx context.Context, transitionRequestsChan chan<- *transitionRequest, fromState schema.TableGCState, fromTableName schema.TableSchemaAndName, isBaseTable bool, uuid string) {
	log.Infof("TableGC: submitting transition request for %s", fromTableName)
	go func() {
		transition := collector.generateTansition(ctx, fromState, fromTableName, isBaseTable, uuid)
		if transition != nil {
			transitionRequestsChan <- transition
		}
	}()
}

// shouldTransitionTable checks if the given table is a GC table and if it's time to transition it to next state
func (collector *TableGC) shouldTransitionTable(tableName string) (shouldTransition bool, state schema.TableGCState, uuid string, err error) {
	isGCTable, state, uuid, t, err := schema.AnalyzeGCTableName(tableName)
	if err != nil {
		return false, state, uuid, err
	}
	if !isGCTable {
		// irrelevant table
		return false, state, uuid, nil
	}
	if _, ok := collector.lifecycleStates[state]; ok {
		// this state is in our expected lifecycle. Let's check table's time hint:
		timeNow := time.Now().UTC()
		if timeNow.Before(t) {
			// not yet time to operate on this table
			return false, state, uuid, nil
		}
		// If the state is not in our expected lifecycle, we ignore the time hint and just move it to the next phase
	}
	return true, state, uuid, nil
}

// checkTables looks for potential GC tables in the MySQL server+schema.
// It lists _vt_% tables, then filters through those which are due-date.
// It then applies the necessary operation per table.
func (collector *TableGC) checkTables(ctx context.Context, dropTablesChan chan<- schema.TableSchemaAndName, transitionRequestsChan chan<- *transitionRequest) error {
	conn, err := collector.pool.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	log.Infof("TableGC: check tables")

	res, err := conn.Exec(ctx, sqlSelectVtTables, math.MaxInt32, true)
	if err != nil {
		return err
	}

	for _, row := range res.Rows {
		fullTblName := schema.NewTableSchemaAndName("", row[0].ToString(), row[1].ToString())
		tableType := row[2].ToString()
		isBaseTable := (tableType == "BASE TABLE")

		shouldTransition, state, uuid, err := collector.shouldTransitionTable(fullTblName.GetTableName())

		if err != nil {
			log.Errorf("TableGC: error while checking tables: %+v", err)
			continue
		}
		if !shouldTransition {
			// irrelevant table
			continue
		}

		log.Infof("TableGC: will operate on table %s", fullTblName.String())

		if state == schema.HoldTableGCState {
			// Hold period expired. Moving to next state
			collector.submitTransitionRequest(ctx, transitionRequestsChan, state, fullTblName, isBaseTable, uuid)
		}
		if state == schema.PurgeTableGCState {
			if isBaseTable {
				// This table needs to be purged. Make sure to enlist it (we may already have)
				collector.addPurgingTable(fullTblName)
			} else {
				// This is a view. We don't need to delete rows from views. Just transition into next phase
				collector.submitTransitionRequest(ctx, transitionRequestsChan, state, fullTblName, isBaseTable, uuid)
			}
		}
		if state == schema.EvacTableGCState {
			// This table was in EVAC state for the required period. It will transition into DROP state
			collector.submitTransitionRequest(ctx, transitionRequestsChan, state, fullTblName, isBaseTable, uuid)
		}
		if state == schema.DropTableGCState {
			// This table needs to be dropped immediately.
			go func() { dropTablesChan <- fullTblName }()
		}
	}

	return nil
}

// purge continuously purges rows from a table.
// This function is non-reentrant: there's only one instance of this function running at any given time.
// A timer keeps calling this function, so if it bails out (e.g. on error) it will later resume work
func (collector *TableGC) purge(ctx context.Context) (fullTblName schema.TableSchemaAndName, err error) {
	if atomic.CompareAndSwapInt64(&purgeReentranceFlag, 0, 1) {
		defer atomic.StoreInt64(&purgeReentranceFlag, 0)
	} else {
		// An instance of this function is already running
		return schema.TableSchemaAndName{}, nil
	}

	fullTblName, found := collector.nextTableToPurge()
	if !found {
		// Nothing do do here...
		return schema.TableSchemaAndName{}, nil
	}

	conn, err := dbconnpool.NewDBConnection(ctx, collector.env.Config().DB.DbaConnector())
	if err != nil {
		return fullTblName, err
	}
	defer conn.Close()

	// Disable binary logging, re-enable afterwards
	// The idea is that DROP TABLE can be expensive, on the primary, if the table is not empty.
	// However, on replica the price is not as high. Therefore, we only purge the rows on the primary.
	// This saves a lot of load from the replication stream, avoiding excessive lags. It also
	// avoids excessive IO on the replicas.
	// (note that the user may skip the PURGE step if they want, but the step is on by default)

	// However, disabling SQL_LOG_BIN requires SUPER privileges, and we don't know that we have that.
	// Any externally managed database might not give SUPER privileges to the vitess accounts, and this is known to be the case for Amazon Aurora.
	// We therefore disable log bin on best-effort basis. The logic is still fine and sound if binary logging
	// is left enabled. We just lose some optimization.
	disableLogBin := func() (bool, error) {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err == nil {
			return true, nil
		}
		if merr, ok := err.(*mysql.SQLError); ok {
			if merr.Num == mysql.ERSpecifiedAccessDenied {
				// We do not have privileges to disable binary logging. That's fine, we're on best effort,
				// so we're going to silently ignore this error.
				return false, nil
			}
		}
		// We do not tolerate other errors, though.
		return false, err
	}
	sqlLogBinDisabled, err := disableLogBin()
	if err != nil {
		return fullTblName, err
	}

	defer func() {
		if sqlLogBinDisabled && !conn.IsClosed() {
			if _, err := conn.ExecuteFetch("SET sql_log_bin = ON", 0, false); err != nil {
				log.Errorf("TableGC: error setting sql_log_bin = ON: %+v", err)
				// a followup defer() will run conn.Close() at any case.
			}
		}
	}()

	log.Infof("TableGC: purge begin for %s", fullTblName)
	for {
		if ctx.Err() != nil {
			// cancelled
			return fullTblName, err
		}
		if !collector.throttlerClient.ThrottleCheckOKOrWait(ctx) {
			continue
		}
		// OK, we're clear to go!

		// Issue a DELETE
		parsed := sqlparser.BuildParsedQuery(sqlPurgeTable, fullTblName.GetTableSchema(), fullTblName.GetTableName())
		res, err := conn.ExecuteFetch(parsed.Query, 1, true)
		if err != nil {
			return fullTblName, err
		}
		if res.RowsAffected == 0 {
			log.Infof("TableGC: purge complete for %s", fullTblName)
			return fullTblName, nil
		}
	}
}

// dropTable runs an actual DROP TABLE statement, and marks the end of the line for the
// tables' GC lifecycle.
func (collector *TableGC) dropTable(ctx context.Context, fullTblName schema.TableSchemaAndName) error {
	conn, err := collector.pool.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	parsed := sqlparser.BuildParsedQuery(sqlDropTable, fullTblName.GetTableSchema(), fullTblName.GetTableName())

	log.Infof("TableGC: dropping table: %s", fullTblName.String())
	_, err = conn.Exec(ctx, parsed.Query, 1, true)
	if err != nil {
		return err
	}
	log.Infof("TableGC: dropped table: %s", fullTblName.String())
	return nil
}

// transitionTable is called upon a transition request. The actual implementation of a transition
// is a RENAME TABLE statement.
func (collector *TableGC) transitionTable(ctx context.Context, transition *transitionRequest) error {
	conn, err := collector.pool.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// when we transition into PURGE, that means we want to begin purging immediately
	// when we transition into DROP, that means we want to drop immediately
	// Thereforce the default timestamp is Now
	t := time.Now().UTC()
	switch transition.toGCState {
	case schema.EvacTableGCState:
		if transition.isBaseTable {
			// in EVAC state  we want the table pages to evacuate from the buffer pool. We therefore
			// set the timestamp to some point the future, which we self determine
			t = t.Add(evacHours * time.Hour)
		}
		// Views don't need evac. t remains "now"
	}

	renameStatement, toTableName, err := schema.GenerateRenameStatementWithUUID(
		transition.fromFullTableName.GetTableSchema(), transition.fromFullTableName.GetTableName(), transition.toGCState, transition.uuid, t)
	if err != nil {
		return err
	}

	log.Infof("TableGC: renaming table: %s to %s", transition.fromFullTableName, toTableName)
	_, err = conn.Exec(ctx, renameStatement, 1, true)
	if err != nil {
		return err
	}
	log.Infof("TableGC: renamed table: %s", transition.fromFullTableName)
	return nil
}

// addPurgingTable adds a table to the list of droppingpurging (or pending purging) tables
func (collector *TableGC) addPurgingTable(tableName schema.TableSchemaAndName) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	collector.purgingTables[tableName] = true
}

// removePurgingTable removes a table from the purging list; likely this is called when
// the table is fully purged and is renamed away to be dropped.
func (collector *TableGC) removePurgingTable(tableName schema.TableSchemaAndName) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	delete(collector.purgingTables, tableName)
}

// nextTableToPurge returns the name of the next table we should start purging.
// We pick the table with the oldest timestamp.
func (collector *TableGC) nextTableToPurge() (tableName schema.TableSchemaAndName, ok bool) {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	if len(collector.purgingTables) == 0 {
		return schema.TableSchemaAndName{}, false
	}
	tableNames := []schema.TableSchemaAndName{}
	for tableName := range collector.purgingTables {
		tableNames = append(tableNames, tableName)
	}
	sort.SliceStable(tableNames, func(i, j int) bool {
		_, _, _, ti, _ := schema.AnalyzeGCTableName(tableNames[i].GetTableName())
		_, _, _, tj, _ := schema.AnalyzeGCTableName(tableNames[j].GetTableName())

		return ti.Before(tj)
	})
	return tableNames[0], true
}

// Status exports a status breakdown
func (collector *TableGC) Status() *Status {
	collector.purgeMutex.Lock()
	defer collector.purgeMutex.Unlock()

	status := &Status{
		Keyspace: "",
		Shard:    "0",

		IsOpen: (atomic.LoadInt64(&collector.isOpen) > 0),
	}
	for table := range collector.purgingTables {
		status.purgingTables = append(status.purgingTables, table.String())
	}

	return status
}
