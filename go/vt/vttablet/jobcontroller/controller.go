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
	databasePoolSize            = 5
	defaultBatchSize            = 1000 //
	monitorInterval             = 5000 // ms
	defaultBatchInterval        = 1    // ms
	tableGCInterval             = 10   // hour
	jobSchedulerRunningInterval = 24   // second
	throttleCheckInterval       = 250  // ms g

	batchSizeThreshold        = 10000
	ratioOfBatchSizeThreshold = 0.5
)

func registerFlags(fs *pflag.FlagSet) {
	fs.IntVar(&databasePoolSize, "non_transactional_dml_database_pool_size", databasePoolSize, "the number of database connection to mysql")
	fs.IntVar(&defaultBatchSize, "non_transactional_dml_default_batch_size", defaultBatchSize, "the number of rows to be processed in one batch by default")
	fs.IntVar(&monitorInterval, "non_transactional_dml_healthcheck_interval", monitorInterval, "the loop interval of job monitor in milliseconds")
	fs.IntVar(&defaultBatchInterval, "non_transactional_dml_default_batch_interval", defaultBatchInterval, "the interval of batch processing in milliseconds by default")
	fs.IntVar(&tableGCInterval, "non_transactional_dml_table_gc_interval", tableGCInterval, "the interval of table GC in hours")
	fs.IntVar(&jobSchedulerRunningInterval, "non_transactional_dml_job_scheduler_running_interval", jobSchedulerRunningInterval, "the interval of job scheduler running in seconds")
	fs.IntVar(&throttleCheckInterval, "non_transactional_dml_throttle_check_interval", throttleCheckInterval, "the interval of throttle check in milliseconds")
	fs.IntVar(&batchSizeThreshold, "non_transactional_dml_batch_size_threshold", batchSizeThreshold, "the	threshold of batch size")
	fs.Float64Var(&ratioOfBatchSizeThreshold, "non_transactional_dml_batch_size_threshold_ratio", ratioOfBatchSizeThreshold, "final threshold = table index numbers * ratio * non_transactional_dml_batch_size_threshold")
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

// 当batch执行失败时的策略，需要注意的时，如果Job在执行batch之外的其他地方发生了错误，则Job会直接变成failed状态，而与failPolicy无关
const (
	failPolicySkip           = "skip"  // 跳过当前batch，继续执行下一个batch
	failPolicyPause          = "pause" // 暂停当前job
	failPolicyAbort          = "abort" // fail当前job
	failPolicyRetryThenPause = "retry_then_pause"

	defaultFailPolicy = failPolicyAbort
)

// possible status of DML job
// batch is status is in ('queued', 'completed')
const (
	postponeLaunchStatus  = "postpone-launch"
	queuedStatus          = "queued"
	runningStatus         = "running"
	pausedStatus          = "paused"
	canceledStatus        = "canceled"
	failedStatus          = "failed"
	completedStatus       = "completed"
	notInTimePeriodStatus = "not-in-time-period"
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

	workingTables      map[string]bool // 用于调度时检测当前任务是否和正在工作的表冲突，paused、running状态的job的表都在里面
	workingTablesMutex sync.Mutex

	schedulerNotifyChan chan struct{} // jobScheduler每隔一段时间运行一次调度。但当它收到这个chan的消息后，会立刻开始一次调度
}

type PKInfo struct {
	pkName string
	pkType querypb.Type
}

type JobRunnerArgs struct {
	uuid, table, tableSchema, batchInfoTable, failPolicy, status string
	batchInterval, batchSize                                     int64
	timePeriodStart, timePeriodEnd                               *time.Time
}

type JobMonitorArgs struct {
	uuid, tableSchema, batchInfoTable, statusSetTime, status, timeZone string
}

func (jc *JobController) Open() error {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		jc.initJobController()
		go jc.jobMonitor()
	}
	return nil
}

func (jc *JobController) initJobController() {
	jc.ctx, jc.cancelOperation = context.WithCancel(context.Background())
	jc.pool.Open(jc.env.Config().DB.AppConnector(), jc.env.Config().DB.DbaConnector(), jc.env.Config().DB.AppDebugConnector())
	jc.workingTables = map[string]bool{}
	jc.schedulerNotifyChan = make(chan struct{}, 1)
	initThrottleTicker()
}

func (jc *JobController) Close() {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	if jc.cancelOperation != nil {
		jc.cancelOperation()
	}
	jc.pool.Close()
	if jc.schedulerNotifyChan != nil {
		close(jc.schedulerNotifyChan)
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

func (jc *JobController) HandleRequest(command, sql, jobUUID, tableSchema, expireString, runningTimePeriodStart, runningTimePeriodEnd string, ratioLiteral *sqlparser.Literal, timeGapInMs, usrBatchSize int64, postponeLaunch bool, failPolicy string) (*sqltypes.Result, error) {
	switch command {
	case SubmitJob:
		return jc.SubmitJob(sql, tableSchema, runningTimePeriodStart, runningTimePeriodEnd, timeGapInMs, usrBatchSize, postponeLaunch, failPolicy)
	case PauseJob:
		return jc.PauseJob(jobUUID)
	case ResumeJob:
		return jc.ResumeJob(jobUUID)
	case LaunchJob:
		return jc.LaunchJob(jobUUID)
	case CancelJob:
		return jc.CancelJob(jobUUID)
	case ThrottleJob:
		return jc.ThrottleJob(jobUUID, expireString, ratioLiteral)
	case UnthrottleJob:
		return jc.UnthrottleJob(jobUUID)
	case SetRunningTimePeriod:
		return jc.SetRunningTimePeriod(jobUUID, runningTimePeriodStart, runningTimePeriodEnd)
	}
	return &sqltypes.Result{}, fmt.Errorf("unknown command: %s", command)
}

func (jc *JobController) SubmitJob(sql, tableSchema, runningTimePeriodStart, runningTimePeriodEnd string, batchIntervalInMs, userBatchSize int64, postponeLaunch bool, failPolicy string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	jobUUID, err := schema.CreateUUIDWithDelimiter("-")
	if err != nil {
		return nil, err
	}
	sql = sqlparser.StripComments(sql)
	if batchIntervalInMs == 0 {
		// todo feat 不设置时间间隔，由throttle进行控制
		batchIntervalInMs = int64(defaultBatchInterval)
	}
	if userBatchSize == 0 {
		userBatchSize = int64(defaultBatchSize)
	}
	// 创建batchInfo表
	tableName, batchInfoTable, batchSize, err := jc.createJobBatches(jobUUID, sql, tableSchema, userBatchSize)
	if err != nil {
		return &sqltypes.Result{}, err
	}
	if batchInfoTable == "" {
		return &sqltypes.Result{}, errors.New("this DML sql won't affect any rows")
	}
	batchInfoTableSchema := tableSchema

	jobStatus := queuedStatus
	if postponeLaunch {
		jobStatus = postponeLaunchStatus
	}
	statusSetTime := time.Now().Format(time.RFC3339)

	// 对runningTimePeriodStart, runningTimePeriodEnd进行有效性检查，需要能够转换成time
	// 当用户没有提交该信息时，默认两个值都为""
	// 当用户用hint提交运维时间时，有可能出现一个为""，一个不为""的情况，因此这里需要用||而不是&&
	if runningTimePeriodStart != "" || runningTimePeriodEnd != "" {
		_, err = time.Parse(time.TimeOnly, runningTimePeriodStart)
		if err != nil {
			return &sqltypes.Result{}, err
		}
		_, err = time.Parse(time.TimeOnly, runningTimePeriodEnd)
		if err != nil {
			return &sqltypes.Result{}, err
		}
	}

	if failPolicy == "" {
		failPolicy = defaultFailPolicy
	} else {
		if failPolicy != failPolicyAbort && failPolicy != failPolicySkip && failPolicy != failPolicyPause {
			return &sqltypes.Result{}, errors.New("failPolicy must be one of 'abort', 'skip' or 'pause'")
		}
	}

	err = jc.insertJobEntry(jobUUID, sql, tableSchema, tableName, batchInfoTableSchema, batchInfoTable,
		jobStatus, statusSetTime, failPolicy, runningTimePeriodStart, runningTimePeriodEnd, batchIntervalInMs, batchSize)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	jc.notifyJobScheduler()

	return jc.buildJobSubmitResult(jobUUID, batchInfoTable, batchIntervalInMs, batchSize, postponeLaunch, failPolicy), nil
}

// 和cancel的区别：1.pasue不会删除元数据 2.cancel状态的job在经过一段时间后会被后台协程回收
// 和cancel的相同点：都停止了runner协程
func (jc *JobController) PauseJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != runningStatus {
		// todo，feat 将info写回给vtgate，目前还不生效
		emptyResult.Info = " The job status is not running and can't be paused"
		return emptyResult, nil
	}

	// 将job在表中的状态改为paused，runner在运行时如果检测到状态不是running，就会退出。
	// pause虽然终止了runner协程，但是
	statusSetTime := time.Now().Format(time.RFC3339)
	qr, err := jc.updateJobStatus(jc.ctx, uuid, pausedStatus, statusSetTime)
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
	if status != pausedStatus {
		emptyResult.Info = " The job status is not paused and don't need resume"
		return emptyResult, nil
	}

	// 准备拉起runner协程的参数
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

	runnerArgs := JobRunnerArgs{}
	runnerArgs.initArgsByQueryResult(row)

	// 拉起runner协程，协程内会将状态改为running
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
	if status != postponeLaunchStatus {
		emptyResult.Info = " The job status is not postpone-launch and don't need launch"
		return emptyResult, nil
	}
	statusSetTime := time.Now().Format(time.RFC3339)
	return jc.updateJobStatus(jc.ctx, uuid, queuedStatus, statusSetTime)
}

func (jc *JobController) CancelJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, nil
	}
	if status == canceledStatus || status == failedStatus || status == completedStatus {
		emptyResult.Info = fmt.Sprintf(" The job status is %s and can't canceld", status)
		return emptyResult, nil
	}
	statusSetTime := time.Now().Format(time.RFC3339)
	qr, err := jc.updateJobStatus(jc.ctx, uuid, canceledStatus, statusSetTime)
	if err != nil {
		return emptyResult, nil
	}

	tableName, _ := jc.getStrJobInfo(jc.ctx, uuid, "table_name")

	// 相比于pause，cancel需要删除内存中的元数据
	jc.deleteDMLJobRunningMeta(tableName)

	jc.notifyJobScheduler()

	return qr, nil
}

func (jc *JobController) CompleteJob(ctx context.Context, uuid, table string) (*sqltypes.Result, error) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()

	statusSetTime := time.Now().Format(time.RFC3339)
	qr, err := jc.updateJobStatus(ctx, uuid, completedStatus, statusSetTime)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	delete(jc.workingTables, table)
	jc.notifyJobScheduler()
	return qr, nil
}

func (jc *JobController) FailJob(ctx context.Context, uuid, message, tableName string) {
	_ = jc.updateJobMessage(ctx, uuid, message)
	statusSetTime := time.Now().Format(time.RFC3339)
	_, _ = jc.updateJobStatus(ctx, uuid, failedStatus, statusSetTime)

	jc.deleteDMLJobRunningMeta(tableName)
	jc.notifyJobScheduler()
}

func (jc *JobController) jobScheduler() {
	// 等待healthcare扫一遍后再进行

	timer := time.NewTicker(time.Duration(jobSchedulerRunningInterval) * time.Hour)
	defer timer.Stop()

	for {
		// 防止vttablet不再是primary时该协程继续执行
		if jc.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
			return
		}
		select {
		case <-jc.ctx.Done():
			return
		case <-timer.C:
		case <-jc.schedulerNotifyChan:
		}

		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()
		// 先提交的job先执行

		qr, _ := jc.execQuery(jc.ctx, "", sqlDMLJobGetJobsToSchedule)
		if qr != nil {
			for _, row := range qr.Named().Rows {
				runnerArgs := JobRunnerArgs{}
				runnerArgs.initArgsByQueryResult(row)

				if jc.checkDmlJobRunnable(runnerArgs.uuid, runnerArgs.status, runnerArgs.table, runnerArgs.timePeriodStart, runnerArgs.timePeriodEnd) {
					// 初始化Job在内存中的元数据，防止在dmlJobBatchRunner修改表中的状态前，scheduler多次启动同一个job
					jc.initDMLJobRunningMeta(runnerArgs.table)
					go jc.dmlJobBatchRunner(runnerArgs.uuid, runnerArgs.table, runnerArgs.tableSchema, runnerArgs.batchInfoTable, runnerArgs.failPolicy, runnerArgs.batchInterval, runnerArgs.batchSize, runnerArgs.timePeriodStart, runnerArgs.timePeriodEnd)
				}
			}
		}

		jc.tableMutex.Unlock()
		jc.workingTablesMutex.Unlock()
	}
}

// todo，feat 可以增加并发Job数的限制
// 调用该函数时外部必须拿tableMutex锁和workingTablesMutex锁
func (jc *JobController) checkDmlJobRunnable(jobUUID, status, table string, periodStartTime, periodEndTime *time.Time) bool {
	if status != queuedStatus && status != notInTimePeriodStatus {
		return false
	}
	if _, exit := jc.workingTables[table]; exit {
		return false
	}
	if periodStartTime != nil && periodEndTime != nil {
		timeNow := time.Now()
		if !(timeNow.After(*periodStartTime) && timeNow.Before(*periodEndTime)) {
			// 更新状态
			submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
				sqltypes.StringBindVariable(notInTimePeriodStatus),
				sqltypes.StringBindVariable(timeNow.Format(time.RFC3339)),
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

	// 1.开启事务
	_, err = conn.Exec(ctx, "start transaction", math.MaxInt32, false)
	// 确保函数意味退出时结束该事务，以释放该事务锁定的资源
	defer func() {
		_, _ = conn.Exec(ctx, "rollback", math.MaxInt32, false)
	}()

	if err != nil {
		return err
	}

	// 2.查询batch sql预计影响的行数，如果超过阈值，则生成新的batch ID
	batchCountSQLForShare := batchCountSQL + " FOR SHARE"
	qr, err := conn.Exec(ctx, batchCountSQLForShare, math.MaxInt32, true)
	if err != nil {
		return err
	}
	if len(qr.Named().Rows) != 1 {
		return errors.New("the len of qr of count expected batch size is not 1")
	}
	expectedRow, _ := qr.Named().Rows[0].ToInt64("count_rows")

	// 检查batch status是否为completed，防止vttablet脑裂问题导致一个batch被多次执行
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
	if batchStatus == completedStatus {
		return nil
	}

	// 将batchID信息记录在系统表中便于用户查看
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

	// 3.执行batch sql
	qr, err = conn.Exec(ctx, batchSQL, math.MaxInt32, true)
	if err != nil {
		return err
	}

	// 4.记录batch sql已经完成，将行数增加到affected rows中
	// 4.1在batch table中记录
	updateBatchStatus := fmt.Sprintf(sqlTempalteUpdateBatchStatusAndAffectedRows, batchTable)
	updateBatchStatusDoneSQL, err := sqlparser.ParseAndBind(updateBatchStatus,
		sqltypes.StringBindVariable(completedStatus),
		sqltypes.Int64BindVariable(int64(qr.RowsAffected)),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, updateBatchStatusDoneSQL, math.MaxInt32, false)
	if err != nil {
		return err
	}

	// 4.2在job表中记录
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

	// 5.提交事务
	_, err = conn.Exec(ctx, "commit", math.MaxInt32, false)
	if err != nil {
		return err
	}
	return nil
}

// 将超过batchSize的batch拆分成两个batches,其中第一个batch的大小等于batchSize
// 拆分的基本原理是遍历原先batch的batchCountSQL的结果集，将第batchSize条record的pk作为原先batch的PKEnd，第batchSize+1条record的pk作为新batch的PKStart
// 原先batch的PKStart和PKEnd分别成为原先batch的PKStart和新batch的PKEnd
func (jc *JobController) splitBatchIntoTwo(ctx context.Context, tableSchema, table, batchTable, batchSQL, batchCountSQL, batchID string, conn *connpool.DBConn, batchSize, expectedRow int64) (newCurrentBatchSQL string, err error) {
	batchSQLStmt, err := sqlparser.Parse(batchSQL)
	if err != nil {
		return "", err
	}
	batchCountSQLStmt, err := sqlparser.Parse(batchCountSQL)
	if err != nil {
		return "", err
	}

	// 1.根据batchCountSQL生成select PKs SQL
	pkInfos, err := jc.getTablePkInfo(ctx, tableSchema, table)
	if err != nil {
		return "", err
	}
	selectPKsSQL := genSelectPKsSQL(batchCountSQLStmt, pkInfos)

	// 2.根据select sql将batch拆分，生成两个新的batch。
	// 这里每次只将超过threshold的batch拆成两个batch而不是多个小于等于threshold的batch的原因是：
	// 拆成多个batch需要遍历完select的全部结果，这可能会导致超时

	// 2.1.计算两个batch的batchPKStart和batchPKEnd。实际上，只要获得当前batch的新的PKEnd和新的batch的PKStart
	// 遍历前threshold+1条，依然使用同一个连接
	var curBatchNewEnd []sqltypes.Value
	var newBatchStart []sqltypes.Value

	qr, err := conn.Exec(ctx, selectPKsSQL, math.MaxInt32, true)
	if err != nil {
		return "", err
	}
	for rowCount, row := range qr.Rows {
		// 将原本batch的PKEnd设在threshold条数处
		if int64(rowCount) == batchSize-1 {
			curBatchNewEnd = row
		}
		// 将第threshold+1条的PK作为新PK的起点
		if int64(rowCount) == batchSize {
			newBatchStart = row
			break
		}
	}
	// 2.2.生成新的batchSQL和新的batchCountSQL
	curBatchSQL, newBatchSQL, newBatchCountSQL, err := genNewBatchSQLsAndCountSQLsWhenSplittingBatch(batchSQLStmt, batchCountSQLStmt, curBatchNewEnd, newBatchStart, pkInfos)
	if err != nil {
		return "", err
	}

	// 2.3.计算两个batch的batch start和end字段
	currentBatchNewBeginStr, currentBatchNewEndStr, newBatchBeginStr, newBatchEndStr, err := getNewBatchesBeginAndEndStr(ctx, conn, batchTable, batchID, curBatchNewEnd, newBatchStart)
	if err != nil {
		return "", err
	}

	// 3 将结果记录在表中：在batch表中更改原本batch的条目的sql，并插入新batch条目
	// 更改原本batch条目
	err = updateBatchInfoTableEntry(ctx, conn, batchTable, curBatchSQL, currentBatchNewBeginStr, currentBatchNewEndStr, batchID)
	if err != nil {
		return "", err
	}
	// 插入新batch条目
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

	_, err := jc.updateJobStatus(jc.ctx, uuid, runningStatus, time.Now().Format(time.RFC3339))
	if err != nil {
		jc.FailJob(jc.ctx, uuid, err.Error(), table)
	}

	// 在一个无限循环中等待定时器触发
	for {
		select {
		case <-jc.ctx.Done():
			return
		case <-timer.C:
		}
		// 防止vttablet不再是primary时该协程继续执行
		if jc.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
			return
		}

		// 定时器触发时执行的函数
		// 检查状态是否为running，可能为paused/canceled
		status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
		if err != nil {
			jc.FailJob(jc.ctx, uuid, err.Error(), table)
			return
		}
		if status != runningStatus {
			return
		}

		// 检查是否在运维窗口内
		// todo feat 增加时区支持，以及是否可能由于脑裂问题导致错误fail掉job?
		if timePeriodStart != nil && timePeriodEnd != nil {
			currentTime := time.Now()
			if !(currentTime.After(*timePeriodStart) && currentTime.Before(*timePeriodEnd)) {
				_, err = jc.updateJobStatus(jc.ctx, uuid, notInTimePeriodStatus, currentTime.Format(time.RFC3339))
				if err != nil {
					jc.FailJob(jc.ctx, uuid, err.Error(), table)
				}
				return
			}
		}

		// 先请求throttle，若被throttle阻塞，则等待下一次timer事件
		if !jc.requestThrottle(uuid) {
			continue
		}

		// 获取本次要执行的batch的batchId
		batchIDToExec, err := jc.getBatchIDToExec(jc.ctx, tableSchema, batchTable)
		if err != nil {
			jc.FailJob(jc.ctx, uuid, err.Error(), table)
			return
		}
		if batchIDToExec == "" {
			// 意味着所有的batch都已经执行完毕，则退出
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

		// 执行当前batch的batch sql，并获得下一要执行的batchID
		err = jc.execBatchAndRecord(jc.ctx, tableSchema, table, batchSQL, batchCountSQL, uuid, batchTable, batchIDToExec, batchSize)
		// 如果执行batch时失败，则根据failPolicy决定处理策略
		if err != nil {
			// todo feat 支持batch并行时需要重新考虑逻辑
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
				_, _ = jc.updateJobStatus(jc.ctx, uuid, pausedStatus, time.Now().Format(time.RFC3339))
				return
				// todo feat 增加retryThenPause策略
			case failPolicyRetryThenPause:
			}
		}
	}
}

// 调用该函数时，需要外部要获取相关的锁
func (jc *JobController) initDMLJobRunningMeta(table string) {
	jc.workingTables[table] = true
}

func (jc *JobController) deleteDMLJobRunningMeta(table string) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()
	delete(jc.workingTables, table)
}

func (jc *JobController) jobMonitor() {
	// 1.启动时，先检查是否有处于"running"或"paused"的job，并恢复它们在内存的状态
	jc.recoverJobsMetadata(jc.ctx)

	log.Info("jobMonitor: metadata of all running and paused jobs are restored to memory\n")
	// 内存状态恢复完毕后，唤醒Job调度协程
	go jc.jobScheduler()

	// 2.每隔一段时间轮询一次，根据job的状态进行不同的处理

	timer := time.NewTicker(time.Duration(monitorInterval) * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-jc.ctx.Done():
			return
		case <-timer.C:
		}
		// 防止vttablet不再是primary时该协程继续执行
		if jc.tabletTypeFunc() != topodatapb.TabletType_PRIMARY {
			return
		}
		jc.tableMutex.Lock()

		qr, _ := jc.execQuery(jc.ctx, "", sqlDMLJobGetAllJobs)
		if qr != nil {
			for _, row := range qr.Named().Rows {
				args := JobMonitorArgs{}
				args.initArgsByQueryResult(row)

				switch args.status {
				// 清理已经运行结束的job的表及条目
				case canceledStatus, failedStatus, completedStatus:
					timeZoneOffset, err := getTimeZoneOffset(args.timeZone)
					if err != nil {
						log.Errorf("jobMonitor: getTimeZoneOffset failed, %s", err)
						continue
					}
					err = jc.tableGC(jc.ctx, args.uuid, args.tableSchema, args.batchInfoTable, args.statusSetTime, timeZoneOffset)
					if err != nil {
						log.Errorf("jobMonitor: tableGC failed, %s", err)
						continue
					}
				case runningStatus:
					// todo feat 增加对长时间未增加rows的running job的处理
				}
			}
		}

		jc.tableMutex.Unlock()

	}
}

func (jc *JobController) recoverJobsMetadata(ctx context.Context) {
	qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
	if qr != nil {
		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()

		for _, row := range qr.Named().Rows {
			status := row["status"].ToString()
			runnerArgs := JobRunnerArgs{}
			runnerArgs.initArgsByQueryResult(row)

			switch status {
			case runningStatus:
				jc.initDMLJobRunningMeta(runnerArgs.table)
				go jc.dmlJobBatchRunner(runnerArgs.uuid, runnerArgs.table, runnerArgs.tableSchema, runnerArgs.batchInfoTable, runnerArgs.failPolicy, runnerArgs.batchInterval, runnerArgs.batchSize, runnerArgs.timePeriodStart, runnerArgs.timePeriodEnd)
			case pausedStatus:
				jc.initDMLJobRunningMeta(runnerArgs.table)
			}
		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
	}
}

func (jc *JobController) tableGC(ctx context.Context, uuid, tableSchema, batchInfoTable, statusSetTime string, timeZoneOffset int) error {
	// job表中的类型为timestamp，其中不记录时区，格式对应着time.Datetime
	statusSetTimeObj, err := time.Parse(time.DateTime, statusSetTime)
	// parse默认使用UTC，故需要将时区调整为表中所记录的时区
	location := time.FixedZone("time zone", timeZoneOffset)
	statusSetTimeObj = time.Date(statusSetTimeObj.Year(), statusSetTimeObj.Month(), statusSetTimeObj.Day(),
		statusSetTimeObj.Hour(), statusSetTimeObj.Minute(), statusSetTimeObj.Second(), statusSetTimeObj.Nanosecond(), location)

	if err != nil {
		return err
	}
	// 如果Job设置结束状态的时间距离当前已经超过了一定的时间间隔，则删除该Job在表中的条目，并将其batch表删除
	if time.Now().After(statusSetTimeObj.Add(time.Duration(tableGCInterval) * time.Second)) {
		deleteJobSQL, err := sqlparser.ParseAndBind(sqlDMLJobDeleteJob,
			sqltypes.StringBindVariable(uuid))
		if err != nil {
			return err
		}
		// 通过sql删除job表中的条目
		_, _ = jc.execQuery(ctx, "", deleteJobSQL)
		// 通过table gc的方式删除batch表，将其设置为PURGE状态（由于在任务完成后已经停留了一段时间，故不再经过HOLD状态）
		_, _ = jc.gcBatchInfoTable(ctx, tableSchema, batchInfoTable, uuid, time.Now())
	}
	return nil
}

// move the table to PURGE_TABLE_GC_STATE state directly
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

func (jc *JobController) createJobBatches(jobUUID, sql, tableSchema string, userBatchSize int64) (tableName, batchTableName string, batchSize int64, err error) {
	// 1.对用户提交的DML sql进行合法性检验和解析
	tableName, whereExpr, stmt, err := parseDML(sql)
	if err != nil {
		return "", "", 0, err
	}
	// 2.检查PK列类型的合法性
	pkInfos, err := jc.getTablePkInfo(jc.ctx, tableSchema, tableName)
	if err != nil {
		return "", "", 0, err
	}
	if existUnSupportedPK(pkInfos) {
		return "", "", 0, errors.New("the table has unsupported PK type")
	}
	// 3.拼接生成selectSQL，用于生成batch表
	pkCols := getPKColsStr(pkInfos)
	selectSQL := fmt.Sprintf("select %s from %s.%s where %s order by %s",
		pkCols, tableSchema, tableName, sqlparser.String(whereExpr), pkCols)

	// 4.计算每个batch的batchSize
	// batchSize = min(userBatchSize, batchSizeThreshold / 每个表的index数量 * ratioOfBatchSizeThreshold)
	indexCount, err := jc.getIndexCount(tableSchema, tableName)
	if err != nil {
		return "", "", 0, err
	}
	actualThreshold := int64(float64(int64(batchSizeThreshold)/indexCount) * ratioOfBatchSizeThreshold)
	if userBatchSize < actualThreshold {
		batchSize = userBatchSize
	} else {
		batchSize = actualThreshold
	}
	// 5.基于selectSQL生成batch表
	batchTableName, err = jc.createBatchTable(jobUUID, selectSQL, tableSchema, sql, tableName, whereExpr, stmt, pkInfos, batchSize)
	return tableName, batchTableName, batchSize, err
}

func (jc *JobController) createBatchTable(jobUUID, selectSQL, tableSchema, sql, tableName string, whereExpr sqlparser.Expr, stmt sqlparser.Statement, pkInfos []PKInfo, batchSize int64) (string, error) {
	// 执行selectSQL，获得有序的pk值结果集，以生成每一个batch要执行的batch SQL
	qr, err := jc.execQuery(jc.ctx, "", selectSQL)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) == 0 {
		return "", nil
	}

	// todo feat 删除batchSQL，batchCountSQL，字段，在内存中生成具体的sql, mysql generate col 或者 go代码实现
	// 为每一个DML job创建一张batch表，保存着该job被拆分成batches的具体信息。
	// healthCheck协程会定时对处于结束状态(completed,canceled,failed)的job的batch表进行回收
	batchTableName := "_vt_BATCH_" + strings.Replace(jobUUID, "-", "_", -1)
	createTableSQL := fmt.Sprintf(sqlTemplateCreateBatchTable, batchTableName)
	_, err = jc.execQuery(jc.ctx, tableSchema, createTableSQL)
	if err != nil {
		return "", err
	}

	// 遍历每一行的每一个PK的值，记录每一个batch的开始和结束pk值（当有多个pk列时，需要记录多个pk值，pk可能具有不同的数据类型
	// 当遍历的行数达到一个batchSize时，即可生成一个batch所要执行的batch SQL，往batch表中插入一个条目
	currentBatchSize := int64(0)
	var currentBatchStart []sqltypes.Value
	var currentBatchEnd []sqltypes.Value
	currentBatchID := "1"

	for _, values := range qr.Rows {
		if currentBatchSize == 0 {
			currentBatchStart = values
		}
		currentBatchEnd = values
		currentBatchSize++
		if currentBatchSize == batchSize {
			batchSQL, countSQL, batchStartStr, batchEndStr, err := createBatchInfoTableEntry(tableSchema, tableName, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
			if err != nil {
				return "", err
			}
			err = jc.insertBatchInfoTableEntry(jc.ctx, tableSchema, batchTableName, currentBatchID, batchSQL, countSQL, batchStartStr, batchEndStr, currentBatchSize)
			if err != nil {
				return "", err
			}
			currentBatchID, err = currentBatchIDInc(currentBatchID)
			if err != nil {
				return "", err
			}
			currentBatchSize = 0
		}
	}
	// 最后一个batch的行数不一定是batchSize，在循环结束时要将剩余的行数划分到最后一个batch中
	if currentBatchSize != 0 {
		batchSQL, countSQL, batchStartStr, batchEndStr, err := createBatchInfoTableEntry(tableSchema, tableName, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
		if err != nil {
			return "", err
		}
		err = jc.insertBatchInfoTableEntry(jc.ctx, tableSchema, batchTableName, currentBatchID, batchSQL, countSQL, batchStartStr, batchEndStr, currentBatchSize)
		if err != nil {
			return "", err
		}
	}
	return batchTableName, nil
}

func createBatchInfoTableEntry(tableSchema, tableName string, sqlStmt sqlparser.Statement, whereExpr sqlparser.Expr,
	currentBatchStart, currentBatchEnd []sqltypes.Value, pkInfos []PKInfo) (batchSQL, countSQL, batchStartStr, batchEndStr string, err error) {
	batchSQL, finalWhereStr, err := genBatchSQL(sqlStmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
	if err != nil {
		return "", "", "", "", err
	}
	countSQL = genCountSQL(tableSchema, tableName, finalWhereStr)
	if err != nil {
		return "", "", "", "", err
	}
	batchStartStr, batchEndStr, err = genBatchStartAndEndStr(currentBatchStart, currentBatchEnd)
	if err != nil {
		return "", "", "", "", err
	}
	return batchSQL, countSQL, batchStartStr, batchEndStr, nil
}

// 通知jobScheduler让它立刻开始一次调度。
func (jc *JobController) notifyJobScheduler() {
	if jc.schedulerNotifyChan == nil {
		return
	}

	// Try to send. If the channel buffer is full, it means a notification is
	// already pending, so we don't need to do anything.
	select {
	case jc.schedulerNotifyChan <- struct{}{}:
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
