/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"github.com/pingcap/failpoint"

	"vitess.io/vitess/go/vt/failpointkey"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"

	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	databasePoolSize          = 3
	defaultBatchSize          = 2000 //
	defaultBatchInterval      = 1    // ms
	tableGCInterval           = 24   // hour
	jobManagerRunningInterval = 24   // second
	throttleCheckInterval     = 250  // ms g
	batchSizeThreshold        = 10000
	ratioOfBatchSizeThreshold = 0.5
)

func registerFlags(fs *pflag.FlagSet) {
	fs.IntVar(&databasePoolSize, "non_transactional_dml_database_pool_size", databasePoolSize, "the number of database connection to mysql")
	fs.IntVar(&defaultBatchSize, "non_transactional_dml_default_batch_size", defaultBatchSize, "the number of rows to be processed in one batch by default")
	fs.IntVar(&defaultBatchInterval, "non_transactional_dml_default_batch_interval", defaultBatchInterval, "the interval of batch processing in milliseconds by default")
	fs.IntVar(&tableGCInterval, "non_transactional_dml_table_gc_interval", tableGCInterval, "the interval of table GC in hours")
	fs.IntVar(&jobManagerRunningInterval, "non_transactional_dml_job_manager_running_interval", jobManagerRunningInterval, "the interval of job scheduler running in seconds")
	fs.IntVar(&throttleCheckInterval, "non_transactional_dml_throttle_check_interval", throttleCheckInterval, "the interval of throttle check in milliseconds")
	fs.IntVar(&batchSizeThreshold, "non_transactional_dml_batch_size_threshold", batchSizeThreshold, "the	threshold of batch size")
	fs.Float64Var(&ratioOfBatchSizeThreshold, "non_transactional_dml_batch_size_threshold_ratio", ratioOfBatchSizeThreshold, "final threshold = ratio * non_transactional_dml_batch_size_threshold / table index numbers")
}

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

// commands for DML job
const (
	SubmitJob            = "submit_job"
	LaunchJob            = "launch"
	LaunchAllJobs        = "launch_all"
	PauseJob             = "pause"
	PauseAllJobs         = "pause_all"
	ResumeJob            = "resume"
	ResumeAllJobs        = "resume_all"
	ThrottleJob          = "throttle"
	ThrottleAllJobs      = "throttle_all"
	UnthrottleJob        = "unthrottle"
	UnthrottleAllJobs    = "unthrottle_all"
	CancelJob            = "cancel"
	SetRunningTimePeriod = "set_running_time_period"
)

// These are strategies when a batch execution fails.
// It's important to note that if a Job encounters an error outside of  batch execution,
// the Job will directly change to failed state, regardless of the failPolicy.
const (
	failPolicySkip           = "skip"  // skip current batch, continue to execute the next one
	failPolicyPause          = "pause" // pause the job util user resume it
	failPolicyAbort          = "abort" // fail the current job
	failPolicyRetryThenPause = "retry_then_pause"

	defaultFailPolicy = failPolicyPause
)

// possible status of DML job
// batch is status is in ('queued', 'completed')
const (
	SubmittedStatus       = "submitted"
	PreparingStatus       = "preparing"
	QueuedStatus          = "queued"
	PostponeLaunchStatus  = "postpone-launch"
	RunningStatus         = "running"
	PausedStatus          = "paused"
	CanceledStatus        = "canceled"
	FailedStatus          = "failed"
	CompletedStatus       = "completed"
	NotInTimePeriodStatus = "not-in-time-period"
)

type JobController struct {
	tableName              string
	tableMutex             sync.Mutex
	tabletTypeFunc         func() topodatapb.TabletType
	env                    tabletenv.Env
	pool                   *connpool.Pool
	lagThrottler           *throttle.Throttler
	lastSuccessfulThrottle int64

	initMutex sync.Mutex

	ctx             context.Context
	cancelOperation context.CancelFunc

	// Used to check the job conflicts when scheduling.
	// The table of jobs which are in the paused or running states will be record.
	workingTables map[string]bool

	workingTablesMutex sync.Mutex

	// The jobManager runs a job schedule every jobManagerRunningInterval seconds.
	// However, when it receives a message from this channel, it will immediately start a schedule.
	managerNotifyChan chan struct{}
}

type PKInfo struct {
	pkName string
	pkType querypb.Type
}

type JobArgs struct {
	uuid, table, tableSchema, batchInfoTable, failPolicy, status, timeZone, statusSetTime, dmlSQL string
	batchInterval, batchSize                                                                      int64
	timePeriodStart, timePeriodEnd                                                                *time.Time
	postponeLaunch                                                                                bool
}

func (jc *JobController) Open() error {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	jc.initJobController()
	go jc.jobManager()

	return nil
}

func (jc *JobController) initJobController() {
	jc.ctx, jc.cancelOperation = context.WithCancel(context.Background())
	jc.pool.Open(jc.env.Config().DB.AppConnector(), jc.env.Config().DB.DbaConnector(), jc.env.Config().DB.AppDebugConnector())
	jc.workingTables = map[string]bool{}
	jc.managerNotifyChan = make(chan struct{}, 1)
	initThrottleTicker()
}

func (jc *JobController) Close() {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	if jc.cancelOperation != nil {
		jc.cancelOperation()
	}
	jc.pool.Close()
	if jc.managerNotifyChan != nil {
		close(jc.managerNotifyChan)
	}
}

func NewJobController(tableName string, tabletTypeFunc func() topodatapb.TabletType, env tabletenv.Env, lagThrottler *throttle.Throttler) *JobController {
	return &JobController{
		tableName:      tableName,
		tabletTypeFunc: tabletTypeFunc,
		env:            env,
		pool: connpool.NewPool(env, "DMLJobControllerPool", tabletenv.ConnPoolConfig{
			Size:               databasePoolSize,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		lagThrottler: lagThrottler}
}

func (jc *JobController) HandleRequest(command, sql, jobUUID, tableSchema, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone, throttleDuration, throttleRatio string, timeGapInMs, usrBatchSize int64, postponeLaunch bool, failPolicy string) (*sqltypes.Result, error) {
	switch command {
	case SubmitJob:
		return jc.SubmitJob(sql, tableSchema, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone, timeGapInMs, usrBatchSize, postponeLaunch, failPolicy, throttleDuration, throttleRatio)
	case PauseJob:
		return jc.PauseJob(jobUUID)
	case ResumeJob:
		return jc.ResumeJob(jobUUID)
	case LaunchJob:
		return jc.LaunchJob(jobUUID)
	case CancelJob:
		return jc.CancelJob(jobUUID)
	case ThrottleJob:
		return jc.ThrottleJob(jobUUID, throttleDuration, throttleRatio)
	case UnthrottleJob:
		return jc.UnthrottleJob(jobUUID)
	case SetRunningTimePeriod:
		return jc.SetRunningTimePeriod(jobUUID, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone)
	}
	return &sqltypes.Result{}, fmt.Errorf("unknown command: %s", command)
}

func (jc *JobController) SubmitJob(sql, tableSchema, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone string, batchIntervalInMs, userBatchSize int64, postponeLaunch bool, failPolicy, throttleDuration, throttleRatio string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	jobUUID, err := schema.CreateUUIDWithDelimiter("-")
	if err != nil {
		return &sqltypes.Result{}, err
	}
	sql = sqlparser.StripComments(sql)
	if batchIntervalInMs == 0 {
		// todo feat: maybe batches can run without interval, just let throttler to decide whether to run
		batchIntervalInMs = int64(defaultBatchInterval)
	}
	if userBatchSize == 0 {
		userBatchSize = int64(defaultBatchSize)
	}
	tableName, batchInfoTable, batchSize, err := jc.initJobBatches(jobUUID, sql, tableSchema, userBatchSize)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	batchInfoTableSchema := tableSchema

	jobStatus := SubmittedStatus

	statusSetTime := time.Now().Format(time.DateTime)

	if failPolicy == "" {
		failPolicy = defaultFailPolicy
	} else {
		if failPolicy != failPolicyAbort && failPolicy != failPolicySkip && failPolicy != failPolicyPause {
			return &sqltypes.Result{}, errors.New("failPolicy must be one of 'abort', 'skip' or 'pause'")
		}
	}

	var throttleExpireAt string
	var throttleRatioFloat64 float64
	if throttleDuration != "" || throttleRatio != "" {
		throttleDuration, throttleRatio = setDefaultValForThrottleParam(throttleDuration, throttleRatio)
		ratioLiteral := sqlparser.NewDecimalLiteral(throttleRatio)
		throttleExpireAt, throttleRatioFloat64, err = jc.ThrottleApp(jobUUID, throttleDuration, ratioLiteral)
		if err != nil {
			return &sqltypes.Result{}, err
		}
	}

	err = jc.insertJobEntry(jobUUID, sql, tableSchema, tableName, batchInfoTableSchema, batchInfoTable,
		jobStatus, statusSetTime, failPolicy, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone, throttleExpireAt, batchIntervalInMs, batchSize, throttleRatioFloat64, postponeLaunch)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	jc.notifyJobManager()

	return jc.buildJobSubmitResult(jobUUID, batchInfoTable, batchIntervalInMs, batchSize, postponeLaunch, failPolicy), nil
}

// The difference between pause and cancel:
// 1. Pause will keep job metadata but cancel won't.
// 2. Jobs in cancel status will get in tableGC but pause won't.
// The similarities between pause and cancel:
// 1. Both stop the runner coroutine.
func (jc *JobController) PauseJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != RunningStatus {
		// todo，feat: send qr.info to vtgate so user can see it
		emptyResult.Info = " The job status is not running and can't be paused"
		return emptyResult, nil
	}

	statusSetTime := time.Now().Format(time.DateTime)
	qr, err := jc.updateJobStatus(jc.ctx, uuid, PausedStatus, statusSetTime)
	if err != nil {
		return emptyResult, err
	}
	return qr, nil
}

func (jc *JobController) ResumeJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != PausedStatus {
		emptyResult.Info = " The job status is not paused and don't need resume"
		return emptyResult, nil
	}

	query, err := sqlparser.ParseAndBind(sqlDMLJobGetInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return emptyResult, err
	}
	rst, err := jc.execQuery(jc.ctx, "", query)
	if err != nil {
		return emptyResult, err
	}
	if len(rst.Named().Rows) != 1 {
		return emptyResult, errors.New("the len of qr of querying job info by uuid is not 1")
	}
	row := rst.Named().Rows[0]

	runnerArgs := JobArgs{}
	runnerArgs.initArgsByQueryResult(row)

	// dmlJobBatchRunner will set the job status to running
	go jc.dmlJobBatchRunner(runnerArgs.uuid, runnerArgs.table, runnerArgs.tableSchema, runnerArgs.batchInfoTable, runnerArgs.failPolicy, runnerArgs.batchInterval, runnerArgs.batchSize, runnerArgs.timePeriodStart, runnerArgs.timePeriodEnd)
	emptyResult.RowsAffected = 1
	return emptyResult, nil
}

func (jc *JobController) LaunchJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != PostponeLaunchStatus {
		emptyResult.Info = " The job status is not postpone-launch and don't need launch"
		return emptyResult, nil
	}
	statusSetTime := time.Now().Format(time.DateTime)
	return jc.updateJobStatus(jc.ctx, uuid, QueuedStatus, statusSetTime)
}

func (jc *JobController) CancelJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status == CanceledStatus || status == FailedStatus || status == CompletedStatus {
		return emptyResult, fmt.Errorf(" The job status is %s and can't canceld", status)
	}
	statusSetTime := time.Now().Format(time.DateTime)
	qr, err := jc.updateJobStatus(jc.ctx, uuid, CanceledStatus, statusSetTime)
	if err != nil {
		return emptyResult, err
	}

	tableName, _ := jc.getStrJobInfo(jc.ctx, uuid, "table_name")

	// compared with pause，cancel need to delete job metadata
	jc.deleteDMLJobRunningMeta(tableName)

	jc.notifyJobManager()

	return qr, nil
}

func (jc *JobController) CompleteJob(ctx context.Context, uuid, table string) (*sqltypes.Result, error) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()

	statusSetTime := time.Now().Format(time.DateTime)
	qr, err := jc.updateJobStatus(ctx, uuid, CompletedStatus, statusSetTime)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	delete(jc.workingTables, table)
	jc.notifyJobManager()
	return qr, nil
}

func (jc *JobController) FailJob(ctx context.Context, uuid, message, tableName string) {
	_ = jc.updateJobMessage(ctx, uuid, message)
	statusSetTime := time.Now().Format(time.DateTime)
	_, _ = jc.updateJobStatus(ctx, uuid, FailedStatus, statusSetTime)

	jc.deleteDMLJobRunningMeta(tableName)
	jc.notifyJobManager()
}

func (jc *JobController) jobManager() {
	// Before jobManager get in infinite loop,
	// it should check whether there are jobs already in 'queued' or 'postpone-launch' or 'paused' or 'running' status
	// and recover their metadata.
	jc.recoverJobsMetadata(jc.ctx)
	log.Info("JobController: metadata of all running and paused jobs are restored to memory\n")

	timer := time.NewTicker(time.Duration(jobManagerRunningInterval) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-jc.ctx.Done():
			return
		case <-timer.C:
		case <-jc.managerNotifyChan:
		}

		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()

		qr, _ := jc.execQuery(jc.ctx, "", sqlDMLJobGetAllJobs)
		if qr != nil {
			for _, row := range qr.Named().Rows {
				jobArgs := JobArgs{}
				jobArgs.initArgsByQueryResult(row)
				switch jobArgs.status {
				case SubmittedStatus:
					if jc.checkIfDmlJobCanPrepare(jobArgs.uuid, jobArgs.status, jobArgs.table, jobArgs.timePeriodStart, jobArgs.timePeriodEnd) {
						// init metadata to prevent two jobs with same table preparing at the same time
						jc.initDMLJobRunningMeta(jobArgs.table)
						// prepare the dml job: init batch info table
						go jc.prepareDMLJob(jobArgs.uuid, jobArgs.dmlSQL, jobArgs.tableSchema, jobArgs.batchInfoTable, jobArgs.batchSize, jobArgs.postponeLaunch)
					}
				case QueuedStatus, NotInTimePeriodStatus:
					if jc.checkDmlJobRunnable(jobArgs.uuid, jobArgs.status, jobArgs.table, jobArgs.timePeriodStart, jobArgs.timePeriodEnd) {
						go jc.dmlJobBatchRunner(jobArgs.uuid, jobArgs.table, jobArgs.tableSchema, jobArgs.batchInfoTable, jobArgs.failPolicy, jobArgs.batchInterval, jobArgs.batchSize, jobArgs.timePeriodStart, jobArgs.timePeriodEnd)
					}
				case CanceledStatus, FailedStatus, CompletedStatus:
					timeZoneOffset, err := getTimeZoneOffset(jobArgs.timeZone)
					if err != nil {
						log.Errorf("jobManager: getTimeZoneOffset failed, %s", err)
						continue
					}
					err = jc.tableGC(jc.ctx, jobArgs.uuid, jobArgs.tableSchema, jobArgs.batchInfoTable, jobArgs.statusSetTime, timeZoneOffset)
					if err != nil {
						log.Errorf("jobManager: tableGC failed, %s", err)
						continue
					}
				case RunningStatus:
					// todo feat: we can do something on Jobs that affect no rows for a long time but in running status
				}

			}

		}

		jc.tableMutex.Unlock()
		jc.workingTablesMutex.Unlock()
	}
}

func (jc *JobController) checkIfDmlJobCanPrepare(jobUUID, status, table string, periodStartTime, periodEndTime *time.Time) bool {
	if status != SubmittedStatus {
		return false
	}
	if _, exit := jc.workingTables[table]; exit {
		return false
	}
	return true
}

// todo feat: we can limit the number of job running concurrently
// acquire 	jc.workingTablesMutex and jc.tableMutex before calling this function
func (jc *JobController) checkDmlJobRunnable(jobUUID, status, table string, periodStartTime, periodEndTime *time.Time) bool {
	if status != QueuedStatus && status != NotInTimePeriodStatus {
		return false
	}
	if periodStartTime != nil && periodEndTime != nil {
		timeNow := time.Now()
		if !(timeNow.After(*periodStartTime) && timeNow.Before(*periodEndTime)) {
			// if current time is not in running time period, we set the job status as "not-in-time-period"
			submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
				sqltypes.StringBindVariable(NotInTimePeriodStatus),
				sqltypes.StringBindVariable(timeNow.Format(time.DateTime)),
				sqltypes.StringBindVariable(jobUUID))
			if err != nil {
				return false
			}
			_, _ = jc.execQuery(jc.ctx, "", submitQuery)
			return false
		}
	}

	return true
}

func (jc *JobController) execBatchAndRecord(ctx context.Context, tableSchema, table, batchSQL, batchCountSQL, uuid, batchTable, batchID string, batchSize int64) (err error) {
	defer jc.env.LogError()

	var setting pools.Setting
	if tableSchema != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", tableSchema))
		setting.SetResetQuery(fmt.Sprintf("use %s", jc.env.Config().DB.DBName))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	defer conn.Recycle()
	failpoint.Inject(failpointkey.CreateErrorWhenExecutingBatch.Name, func(val failpoint.Value) {
		temp, ok := val.(bool)
		if ok && temp {
			err = errors.New("error created by failpoint")
		}
	})

	if err != nil {
		return err
	}

	// 1. start a transaction
	_, err = conn.Exec(ctx, "start transaction", math.MaxInt32, false)
	// ensure the resource acquired by the transaction is released when the function exits
	defer func() {
		_, _ = conn.Exec(ctx, "rollback", math.MaxInt32, false)
	}()

	if err != nil {
		return err
	}

	// 2. Query the number of rows that is going to be affected by this batch SQL.
	// If it exceeds the threshold, we should split it.
	// Here we use "FOR SHARE" to prevent users from modifying rows related to this batch.
	batchCountSQLForShare := batchCountSQL + " FOR SHARE"
	qr, err := conn.Exec(ctx, batchCountSQLForShare, math.MaxInt32, true)
	if err != nil {
		return err
	}
	if len(qr.Named().Rows) != 1 {
		return errors.New("the len of qr of count expected batch size is not 1")
	}
	expectedRow, _ := qr.Named().Rows[0].ToInt64("count_rows")

	// Check if the batch status is "completed" to prevent potential issues caused by vttablet brain split,
	// ensuring a batch is not executed multiple times.
	sqlGetBatchStatus := fmt.Sprintf(sqlTemplateGetBatchStatus, batchTable)
	queryGetBatchStatus, err := sqlparser.ParseAndBind(sqlGetBatchStatus, sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	qr, err = conn.Exec(ctx, queryGetBatchStatus, math.MaxInt32, true)
	if err != nil {
		return err
	}
	if len(qr.Named().Rows) != 1 {
		return errors.New("the len of qr of count expected batch size is not 1")
	}
	batchStatus, _ := qr.Named().Rows[0].ToString("batch_status")
	if batchStatus == CompletedStatus {
		return nil
	}

	// Record the batch ID information in the non_transactional_dml_jobs table for user reference.
	queryUpdateDealingBatchID, err := sqlparser.ParseAndBind(sqlUpdateDealingBatchID,
		sqltypes.StringBindVariable(batchID),
		sqltypes.StringBindVariable(uuid))
	_, err = conn.Exec(ctx, queryUpdateDealingBatchID, math.MaxInt32, false)
	if err != nil {
		return err
	}

	// this failpoint is used to test splitBatchIntoTwo
	failpoint.Inject(failpointkey.ModifyBatchSize.Name, func(val failpoint.Value) {
		temp, ok := val.(int)
		if ok {
			batchSize = int64(temp)
		}
	})
	if expectedRow > batchSize {
		batchSQL, err = jc.splitBatchIntoTwo(ctx, tableSchema, table, batchTable, batchSQL, batchCountSQL, batchID, conn, batchSize, expectedRow)
		if err != nil {
			return err
		}
	}

	// 3.Execute the batch SQL.
	qr, err = conn.Exec(ctx, batchSQL, math.MaxInt32, true)
	if err != nil {
		return err
	}

	// 4.Record the executing result, adding the row count to the affected rows.
	// 4.1 Record in the batch table.
	updateBatchStatus := fmt.Sprintf(sqlTempalteUpdateBatchStatusAndAffectedRows, batchTable)
	updateBatchStatusDoneSQL, err := sqlparser.ParseAndBind(updateBatchStatus,
		sqltypes.StringBindVariable(CompletedStatus),
		sqltypes.Int64BindVariable(int64(qr.RowsAffected)),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, updateBatchStatusDoneSQL, math.MaxInt32, false)
	if err != nil {
		return err
	}

	// 4.2 Record in the job table.
	updateAffectedRowsSQL, err := sqlparser.ParseAndBind(sqlDMLJobUpdateAffectedRows,
		sqltypes.Int64BindVariable(int64(qr.RowsAffected)),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	_, err = conn.Exec(ctx, updateAffectedRowsSQL, math.MaxInt32, false)
	if err != nil {
		return err
	}

	// 5.Commit the transaction.
	_, err = conn.Exec(ctx, "commit", math.MaxInt32, false)
	if err != nil {
		return err
	}
	return nil
}

// Split batches that larger than batchSize into two batches, with the first batch having a size equal to batchSize.
// The basic principle of the splitting is to iterate through the query result set of batchCountSQL of the original batch.
// Take the primary key (pk) of the batchSize-th record as the original batch's PKEnd and the primary key of the (batchSize+1)-th record as the PKStart for the new batch.
// The original batch's PKStart becomes the PKStart for the original batch, and the PKEnd becomes the PKEnd for the new batch.
func (jc *JobController) splitBatchIntoTwo(ctx context.Context, tableSchema, table, batchTable, batchSQL, batchCountSQL, batchID string, conn *connpool.DBConn, batchSize, expectedRow int64) (newCurrentBatchSQL string, err error) {
	batchSQLStmt, err := sqlparser.Parse(batchSQL)
	if err != nil {
		return "", err
	}
	batchCountSQLStmt, err := sqlparser.Parse(batchCountSQL)
	if err != nil {
		return "", err
	}

	// 1. Generate SQL to select primary keys based on batchCountSQL.
	pkInfos, err := jc.getTablePkInfo(ctx, tableSchema, table)
	if err != nil {
		return "", err
	}
	selectPKsSQL := genSelectPKsSQLByBatchCountSQL(batchCountSQLStmt, pkInfos)

	// 2. Split the batch into two new batches using the select SQL.
	// The reason for splitting batches exceeding the threshold into only two batches is that:
	// splitting into multiple batches requires iterating through the entire result set of the select sql,
	// which could lead to timeouts.

	// 2.1. Calculate batchPKStart and batchPKEnd for the two batches.
	// Actually, only need to obtain the new PKEnd for the current batch and the PKStart for the new batch.
	var curBatchNewEnd []sqltypes.Value
	var newBatchStart []sqltypes.Value

	qr, err := conn.Exec(ctx, selectPKsSQL, math.MaxInt32, true)
	if err != nil {
		return "", err
	}
	for rowCount, row := range qr.Rows {
		// Take pk of the batchSize-th record as the original batch's PKEnd
		if int64(rowCount) == batchSize-1 {
			curBatchNewEnd = row
		}
		// Take pk of the (batchSize+1)-th record as the PKStart for the new batch.
		if int64(rowCount) == batchSize {
			newBatchStart = row
			break
		}
	}
	// 2.2. Generate new batchSQL and new batchCountSQL.
	curBatchSQL, newBatchSQL, newBatchCountSQL, err := genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	if err != nil {
		return "", err
	}

	// 2.3. Calculate the batch start and end fields for the two batches.
	currentBatchNewBeginStr, currentBatchNewEndStr, newBatchBeginStr, newBatchEndStr, err := getNewBatchesBeginAndEndStr(ctx, conn, batchTable, batchID, curBatchNewEnd, newBatchStart)
	if err != nil {
		return "", err
	}

	// 3. Record the results in the table:
	// Update the SQL for the original batch entries in the batch table and insert a new entry for the new batch.
	// 3.1 Update the entries for the original batch.
	err = updateBatchInfoTableEntry(ctx, conn, batchTable, curBatchSQL, currentBatchNewBeginStr, currentBatchNewEndStr, batchID)
	if err != nil {
		return "", err
	}
	// 3.2 Insert a new entry for the new batch.
	nextBatchID, err := genNewBatchID(batchID)
	if err != nil {
		return "", err
	}
	newBatchSize := expectedRow - batchSize
	err = insertBatchInfoTableEntry(ctx, conn, batchTable, nextBatchID, newBatchSQL, newBatchCountSQL, newBatchBeginStr, newBatchEndStr, newBatchSize)
	if err != nil {
		return "", err
	}

	newCurrentBatchSQL = curBatchSQL
	return newCurrentBatchSQL, nil
}

func (jc *JobController) dmlJobBatchRunner(uuid, table, tableSchema, batchTable, failPolicy string, batchInterval, batchSize int64, timePeriodStart, timePeriodEnd *time.Time) {

	timer := time.NewTicker(time.Duration(batchInterval) * time.Millisecond)
	defer timer.Stop()

	_, err := jc.updateJobStatus(jc.ctx, uuid, RunningStatus, time.Now().Format(time.DateTime))
	if err != nil {
		jc.FailJob(jc.ctx, uuid, err.Error(), table)
	}

	for {
		select {
		case <-jc.ctx.Done():
			return
		case <-timer.C:
		}
		status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
		if err != nil {
			jc.FailJob(jc.ctx, uuid, err.Error(), table)
			return
		}
		// if the job is paused or canceled by user, just return
		if status != RunningStatus {
			return
		}

		// check whether current time is in running time period
		if timePeriodStart != nil && timePeriodEnd != nil {
			currentTime := time.Now()
			if !(currentTime.After(*timePeriodStart) && currentTime.Before(*timePeriodEnd)) {
				_, err = jc.updateJobStatus(jc.ctx, uuid, NotInTimePeriodStatus, currentTime.Format(time.DateTime))
				if err != nil {
					jc.FailJob(jc.ctx, uuid, err.Error(), table)
				}
				return
			}
		}

		// request throttler
		if !jc.requestThrottle(uuid) {
			continue
		}

		// get batchID of batch to execute now
		batchIDToExec, err := jc.getBatchIDToExec(jc.ctx, tableSchema, batchTable)
		if err != nil {
			jc.FailJob(jc.ctx, uuid, err.Error(), table)
			return
		}
		if batchIDToExec == "" {
			// it means that all batches are finished, so we can complete the job
			_, err = jc.CompleteJob(jc.ctx, uuid, table)
			if err != nil {
				jc.FailJob(jc.ctx, uuid, err.Error(), table)
			}
			return
		}

		batchSQL, batchCountSQL, err := jc.getBatchSQLsByID(jc.ctx, batchIDToExec, batchTable, tableSchema)
		if err != nil {
			jc.FailJob(jc.ctx, uuid, err.Error(), table)
			return
		}

		// execute the batchSQL and record the result in a transaction
		err = jc.execBatchAndRecord(jc.ctx, tableSchema, table, batchSQL, batchCountSQL, uuid, batchTable, batchIDToExec, batchSize)
		// if the batch fails, do something according to the failPolicy
		if err != nil {
			// todo feat: if we support concurrency in batch level, we should redesign the code logic here
			switch failPolicy {
			case failPolicyAbort:
				jc.FailJob(jc.ctx, uuid, err.Error(), table)
				return
			case failPolicySkip:
				_ = jc.updateBatchStatus(tableSchema, batchTable, failPolicySkip, batchIDToExec, err.Error())
				continue
			case failPolicyPause:
				msg := fmt.Sprintf("batch %s failed, pause job: %s", batchIDToExec, err.Error())
				_ = jc.updateJobMessage(jc.ctx, uuid, msg)
				_, _ = jc.updateJobStatus(jc.ctx, uuid, PausedStatus, time.Now().Format(time.DateTime))
				return
				// todo feat: we can retry a certain times before pausing the job
			case failPolicyRetryThenPause:
			}
		}
	}
}

// acquire jc.workingTablesMutex before calling this function
func (jc *JobController) initDMLJobRunningMeta(table string) {
	jc.workingTables[table] = true
}

func (jc *JobController) deleteDMLJobRunningMeta(table string) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()
	delete(jc.workingTables, table)
}

func (jc *JobController) recoverJobsMetadata(ctx context.Context) {
	qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
	if qr != nil {
		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()

		for _, row := range qr.Named().Rows {
			status := row["status"].ToString()
			jobArgs := JobArgs{}
			jobArgs.initArgsByQueryResult(row)

			switch status {
			case PreparingStatus:
				jc.initDMLJobRunningMeta(jobArgs.table)
				go jc.prepareDMLJob(jobArgs.uuid, jobArgs.dmlSQL, jobArgs.tableSchema, jobArgs.batchInfoTable, jobArgs.batchSize, jobArgs.postponeLaunch)
			case QueuedStatus, NotInTimePeriodStatus, PausedStatus:
				jc.initDMLJobRunningMeta(jobArgs.table)
			case RunningStatus:
				jc.initDMLJobRunningMeta(jobArgs.table)
				go jc.dmlJobBatchRunner(jobArgs.uuid, jobArgs.table, jobArgs.tableSchema, jobArgs.batchInfoTable, jobArgs.failPolicy, jobArgs.batchInterval, jobArgs.batchSize, jobArgs.timePeriodStart, jobArgs.timePeriodEnd)
			}
		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
	}
}

func (jc *JobController) tableGC(ctx context.Context, uuid, tableSchema, batchInfoTable, statusSetTime string, timeZoneOffset int) error {
	// Because we recode both datetime and timezone in job table, so we can recover the time data correctly
	statusSetTimeObj, err := time.Parse(time.DateTime, statusSetTime)
	location := time.FixedZone("time zone", timeZoneOffset)
	statusSetTimeObj = time.Date(statusSetTimeObj.Year(), statusSetTimeObj.Month(), statusSetTimeObj.Day(),
		statusSetTimeObj.Hour(), statusSetTimeObj.Minute(), statusSetTimeObj.Second(), statusSetTimeObj.Nanosecond(), location)

	if err != nil {
		return err
	}
	// we delete job entry and drop batch table (by table gc) of the jobs that have been finished for a while
	if time.Now().After(statusSetTimeObj.Add(time.Duration(tableGCInterval) * time.Hour)) {
		deleteJobSQL, err := sqlparser.ParseAndBind(sqlDMLJobDeleteJob,
			sqltypes.StringBindVariable(uuid))
		if err != nil {
			return err
		}
		// delete job entry by SQL
		_, _ = jc.execQuery(ctx, "", deleteJobSQL)
		// delete batch table by table gc: set the table as "PURGE" status
		_, _ = jc.gcBatchInfoTable(ctx, tableSchema, batchInfoTable, uuid, time.Now().UTC())
	}
	return nil
}

// move the table to PURGE_TABLE_GC_STATE state
func (jc *JobController) gcBatchInfoTable(ctx context.Context, tableSchema, artifactTable, uuid string, t time.Time) (string, error) {
	tableExists, err := jc.tableExists(ctx, tableSchema, artifactTable)
	if err != nil {
		return "", err
	}
	if !tableExists {
		return "", nil
	}

	renameStatement, toTableName, err := schema.GenerateRenameStatementWithUUID(tableSchema, artifactTable, schema.PurgeTableGCState, nonTransactionalDMLToGCUUID(uuid), t)
	if err != nil {
		return toTableName, err
	}
	_, err = jc.execQuery(ctx, tableSchema, renameStatement)
	return toTableName, err
}

func (jc *JobController) prepareDMLJob(jobUUID, sql, tableSchema, batchTableName string, batchSize int64, postponeLaunch bool) {
	// 1.Validate and parse the DML SQL submitted by the user.
	tableName, whereExpr, stmt, err := parseDML(sql)
	if err != nil {
		jc.FailJob(jc.ctx, jobUUID, err.Error(), tableName)
	}
	// 2.Validate the PK columns types.
	pkInfos, err := jc.getTablePkInfo(jc.ctx, tableSchema, tableName)
	if err != nil {
		jc.FailJob(jc.ctx, jobUUID, err.Error(), tableName)
	}
	if existUnSupportedPK(pkInfos) {
		jc.FailJob(jc.ctx, jobUUID, "the table has unsupported PK type", tableName)
	}

	// 3.All validation are done, set job status to "preparing"
	_, err = jc.updateJobStatus(jc.ctx, jobUUID, PreparingStatus, time.Now().Format(time.DateTime))
	if err != nil {
		jc.FailJob(jc.ctx, jobUUID, err.Error(), tableName)
	}

	// 4.Generate selectPksSQL which are used for creating the batch table.
	selectPksSQL := sprintfSelectPksSQL(tableName, sqlparser.String(whereExpr), pkInfos)

	// 5.Generate the batch table based on the selectPksSQL.
	err = jc.createBatchTable(jobUUID, selectPksSQL, tableSchema, tableName, batchTableName, whereExpr, stmt, pkInfos, batchSize)
	if err != nil {
		jc.FailJob(jc.ctx, jobUUID, err.Error(), tableName)
	}
	// 6.Set job status to "queued" or "postpone launch"
	if postponeLaunch {
		_, err = jc.updateJobStatus(jc.ctx, jobUUID, PostponeLaunchStatus, time.Now().Format(time.DateTime))
	} else {
		_, err = jc.updateJobStatus(jc.ctx, jobUUID, QueuedStatus, time.Now().Format(time.DateTime))
	}
	if err != nil {
		jc.FailJob(jc.ctx, jobUUID, err.Error(), tableName)
	}
	jc.notifyJobManager()
}

func (jc *JobController) initJobBatches(jobUUID, sql, tableSchema string, userBatchSize int64) (tableName, batchTableName string, batchSize int64, err error) {
	// 1.Validate and parse the DML SQL submitted by the user.
	tableName, _, _, err = parseDML(sql)
	if err != nil {
		return "", "", 0, err
	}

	// 2.Calculate the batchSize for each batch.
	// batchSize = min(userBatchSize, batchSizeThreshold / 每个表的index数量 * ratioOfBatchSizeThreshold)
	indexCount, err := jc.getIndexCount(tableSchema, tableName)
	if err != nil {
		return "", "", 0, err
	}
	actualThreshold := int64(float64(batchSizeThreshold/indexCount) * ratioOfBatchSizeThreshold)
	if userBatchSize < actualThreshold {
		batchSize = userBatchSize
	} else {
		batchSize = actualThreshold
	}
	// 3.Generate the batch table name
	batchTableName = "_vt_BATCH_" + strings.Replace(jobUUID, "-", "_", -1)
	return tableName, batchTableName, batchSize, err
}

func (jc *JobController) createBatchTable(jobUUID, selectSQL, tableSchema, tableName, batchTableName string, whereExpr sqlparser.Expr, stmt sqlparser.Statement, pkInfos []PKInfo, batchSize int64) error {
	// Execute selectSQL to obtain an ordered result set of PK values
	// which are used to generate batch SQL for each batch.
	qr, err := jc.execQuery(jc.ctx, tableSchema, selectSQL)
	if err != nil {
		return err
	}
	if len(qr.Named().Rows) == 0 {
		return errors.New("this DML sql won't affect any rows")
	}

	// todo feat: maybe we don't need to store batchSQL and batchCountSQL in system table, just generate them during user query, by Go or Mysql

	// For each DML job, create a batch info table records specific information about how the job is divided into batches.
	// Drop table before creating to make sure that this function is reentrant
	DropTableSQL := fmt.Sprintf(sqlTemplateDropBatchTable, batchTableName)
	_, _ = jc.execQuery(jc.ctx, tableSchema, DropTableSQL)

	createTableSQL := fmt.Sprintf(sqlTemplateCreateBatchTable, batchTableName)
	_, err = jc.execQuery(jc.ctx, tableSchema, createTableSQL)
	if err != nil {
		return err
	}

	// Iterate through each row and each PK value,
	// recording the start and end PK values for each batch (maybe more than one PK columns and types).
	currentBatchSize := int64(0)
	var currentBatchStart []sqltypes.Value
	var currentBatchEnd []sqltypes.Value
	currentBatchID := "1"

	i := 0
	for i < len(qr.Rows) {
		if !jc.requestThrottle(jobUUID) {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		values := qr.Rows[i]
		if currentBatchSize == 0 {
			currentBatchStart = values
		}
		currentBatchEnd = values
		currentBatchSize++
		// When the number of rows reaches a batchSize,
		// generate a batch SQL to be executed for this batch, and insert an entry into the batch table.
		if currentBatchSize == batchSize {
			batchSQL, countSQL, batchStartStr, batchEndStr, err := createBatchInfoTableEntry(tableName, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
			if err != nil {
				return err
			}
			err = jc.insertBatchInfoTableEntry(jc.ctx, tableSchema, batchTableName, currentBatchID, batchSQL, countSQL, batchStartStr, batchEndStr, currentBatchSize)
			if err != nil {
				return err
			}
			currentBatchID, err = currentBatchIDInc(currentBatchID)
			if err != nil {
				return err
			}
			currentBatchSize = 0
		}
		i++
	}
	// The number of rows in the last batch may less than batchSize:
	// any remaining rows should be allocated to the last batch at the end of the loop.
	if currentBatchSize != 0 {
		batchSQL, countSQL, batchStartStr, batchEndStr, err := createBatchInfoTableEntry(tableName, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
		if err != nil {
			return err
		}
		err = jc.insertBatchInfoTableEntry(jc.ctx, tableSchema, batchTableName, currentBatchID, batchSQL, countSQL, batchStartStr, batchEndStr, currentBatchSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func createBatchInfoTableEntry(tableName string, sqlStmt sqlparser.Statement, whereExpr sqlparser.Expr,
	currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (batchSQL, countSQL, batchStartStr, batchEndStr string, err error) {
	batchSQL, finalWhereStr, err := genBatchSQL(sqlStmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
	if err != nil {
		return "", "", "", "", err
	}
	countSQL = genCountSQL(tableName, finalWhereStr)
	if err != nil {
		return "", "", "", "", err
	}
	batchStartStr, batchEndStr, err = genBatchStartAndEndStr(currentBatchStart, currentBatchEnd)
	if err != nil {
		return "", "", "", "", err
	}
	return batchSQL, countSQL, batchStartStr, batchEndStr, nil
}

// notify jobManager to run schedule immediately
func (jc *JobController) notifyJobManager() {
	if jc.managerNotifyChan == nil {
		return
	}

	// Try to send. If the channel buffer is full, it means a notification is
	// already pending, so we don't need to do anything.
	select {
	case jc.managerNotifyChan <- struct{}{}:
	default:
	}
}

func existUnSupportedPK(pkInfos []PKInfo) bool {
	for _, pk := range pkInfos {
		switch pk.pkType {
		case sqltypes.Float64, sqltypes.Float32, sqltypes.Decimal,
			sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary, sqltypes.Bit,
			sqltypes.Text,
			sqltypes.Enum, sqltypes.Set, sqltypes.Tuple, sqltypes.Geometry, sqltypes.TypeJSON, sqltypes.Expression,
			sqltypes.HexNum, sqltypes.HexVal, sqltypes.BitNum:
			return true
		}
	}
	return false
}
