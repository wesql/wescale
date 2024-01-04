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
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"

	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// config
const (
	databasePoolSize = 5
	defaultBatchSize = 100

	healthCheckInterval         = 5000 * time.Millisecond // ms
	defaultBatchInterval        = 1000                    // 1000ms
	tableEntryGCInterval        = 24 * time.Hour
	jobSchedulerRunningInterval = 10 * time.Second
	throttleCheckInterval       = 250 * time.Millisecond

	batchSizeThreshold        = 10000
	ratioOfBatchSizeThreshold = 0.5
)

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
	failPolicySkip  = "skip"  // 跳过当前batch，继续执行下一个batch
	failPolicyPause = "pause" // 暂停当前job
	failPolicyAbort = "abort" // fail当前job

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

	workingTables      map[string]bool // 用于调度时检测当前任务是否和正在工作的表冲突，paused、running状态的job的表都在里面
	workingTablesMutex sync.Mutex

	checkBeforeSchedule chan struct{} // 用于确保当healthCheck拉起crash的running job的runner协程后，job scheduler才开始运行

	schedulerNotifyChan chan struct{} // jobScheduler每隔一段时间运行一次调度。但当它收到这个chan的消息后，会立刻开始一次调度
}

type PKInfo struct {
	pkName string
	pkType querypb.Type
}

func (jc *JobController) Open() error {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		jc.pool.Open(jc.env.Config().DB.AppConnector(), jc.env.Config().DB.DbaConnector(), jc.env.Config().DB.AppDebugConnector())

		jc.workingTables = map[string]bool{}
		jc.checkBeforeSchedule = make(chan struct{})
		jc.schedulerNotifyChan = make(chan struct{}, 1)

		go jc.jobHealthCheck(jc.checkBeforeSchedule)
		go jc.jobScheduler(jc.checkBeforeSchedule)
		initThrottleTicker()

	}
	return nil
}

func (jc *JobController) Close() {
	jc.initMutex.Lock()
	defer jc.initMutex.Unlock()
	jc.pool.Close()
	if jc.checkBeforeSchedule != nil {
		close(jc.checkBeforeSchedule)
	}
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

func (jc *JobController) SubmitJob(sql, tableSchema, runningTimePeriodStart, runningTimePeriodEnd string, timeGapInMs, userBatchSize int64, postponeLaunch bool, failPolicy string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	ctx := context.Background()

	jobUUID, err := schema.CreateUUIDWithDelimiter("-")
	if err != nil {
		return nil, err
	}
	sql = sqlparser.StripComments(sql)
	if timeGapInMs == 0 {
		timeGapInMs = int64(defaultBatchInterval)
	}
	if userBatchSize == 0 {
		userBatchSize = int64(defaultBatchSize)
	}
	// 取用户输入的batchSize和程序的threshold的最小值作为每个batch最终的batchSize
	var batchSize int64

	if userBatchSize < batchSizeThreshold {
		batchSize = userBatchSize
	} else {
		batchSize = batchSizeThreshold
	}

	tableName, batchInfoTable, batchSize, err := jc.createJobBatches(jobUUID, sql, tableSchema, batchSize)
	batchInfoTableSchema := tableSchema

	if err != nil {
		return &sqltypes.Result{}, err
	}
	if batchInfoTable == "" {
		return &sqltypes.Result{}, errors.New("this DML sql won't affect any rows")
	}

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

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobSubmit,
		sqltypes.StringBindVariable(jobUUID),
		sqltypes.StringBindVariable(sql),
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(batchInfoTableSchema),
		sqltypes.StringBindVariable(batchInfoTable),
		sqltypes.Int64BindVariable(timeGapInMs),
		sqltypes.Int64BindVariable(batchSize),
		sqltypes.StringBindVariable(jobStatus),
		sqltypes.StringBindVariable(statusSetTime),
		sqltypes.StringBindVariable(failPolicy),
		sqltypes.StringBindVariable(runningTimePeriodStart),
		sqltypes.StringBindVariable(runningTimePeriodEnd))
	if err != nil {
		return nil, err
	}

	_, err = jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	jc.notifyJobScheduler()

	return jc.buildJobSubmitResult(jobUUID, batchInfoTable, timeGapInMs, userBatchSize, postponeLaunch, failPolicy), nil
}

func (jc *JobController) buildJobSubmitResult(jobUUID, jobBatchTable string, timeGap, subtaskRows int64, postponeLaunch bool, failPolicy string) *sqltypes.Result {
	var rows []sqltypes.Row
	row := buildVarCharRow(jobUUID, jobBatchTable, strconv.FormatInt(timeGap, 10), strconv.FormatInt(subtaskRows, 10), failPolicy, strconv.FormatBool(postponeLaunch))
	rows = append(rows, row)
	submitRst := &sqltypes.Result{
		Fields:       buildVarCharFields("job_uuid", "batch_info_table_name", "time_gap_in_ms", "batch_size", "fail_policy", "postpone_launch"),
		Rows:         rows,
		RowsAffected: 1,
	}
	return submitRst
}

// 和cancel的区别：1.pasue不会删除元数据 2.cancel状态的job在经过一段时间后会被后台协程回收
// 和cancel的相同点：都停止了runner协程
func (jc *JobController) PauseJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != runningStatus {
		// todo，将info写回给vtgate，目前还不生效
		emptyResult.Info = " The job status is not running and can't be paused"
		return emptyResult, nil
	}

	// 将job在表中的状态改为paused，runner在运行时如果检测到状态不是running，就会退出。
	// pause虽然终止了runner协程，但是
	statusSetTime := time.Now().Format(time.RFC3339)
	qr, err := jc.updateJobStatus(ctx, uuid, pausedStatus, statusSetTime)
	if err != nil {
		return emptyResult, err
	}
	return qr, nil
}

func (jc *JobController) ResumeJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "status")
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
	rst, err := jc.execQuery(ctx, "", query)
	if err != nil {
		return emptyResult, err
	}
	if len(rst.Named().Rows) != 1 {
		return emptyResult, errors.New("the len of qr of querying job info by uuid is not 1")
	}
	row := rst.Named().Rows[0]
	tableSchema := row["table_schema"].ToString()
	table := row["table_name"].ToString()
	jobBatchTable := row["batch_info_table_name"].ToString()
	batchInterval, _ := row["batch_interval_in_ms"].ToInt64()
	batchSize, _ := row["batch_szie"].ToInt64()
	runningTimePeriodStart := row["running_time_period_start"].ToString()
	runningTimePeriodEnd := row["running_time_period_end"].ToString()
	periodStartTimePtr, periodEndTimePtr := getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd)
	failPolicy := row["fail_policy"].ToString()

	// 拉起runner协程，协程内会将状态改为running
	go jc.dmlJobBatchRunner(uuid, table, tableSchema, jobBatchTable, failPolicy, batchInterval, batchSize, periodStartTimePtr, periodEndTimePtr)
	emptyResult.RowsAffected = 1
	return emptyResult, nil
}

func (jc *JobController) SetRunningTimePeriod(uuid, startTime, endTime string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()

	// 如果两个时间只有一个为空，则报错
	if (startTime == "" && endTime != "") || (startTime != "" && endTime == "") {
		return emptyResult, errors.New("the start time and end time must be both set or not")
	}

	status, err := jc.GetStrJobInfo(ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status == runningStatus {
		return emptyResult, errors.New("the job is running now, pause it first")
	}
	// 提交的时间段必须满足特定的格式，可以成功转换成time对象
	if startTime != "" && endTime != "" {
		_, err = time.Parse(time.TimeOnly, startTime)
		if err != nil {
			return emptyResult, errors.New("the start time is in error format, it should be like HH:MM:SS")
		}
		_, err = time.Parse(time.TimeOnly, endTime)
		if err != nil {
			return emptyResult, errors.New("the start time is in error format, it should be like HH:MM:SS")
		}
	}
	// 往表中插入
	return jc.updateJobPeriodTime(ctx, uuid, startTime, endTime)
}

func (jc *JobController) LaunchJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status != postponeLaunchStatus {
		emptyResult.Info = " The job status is not postpone-launch and don't need launch"
		return emptyResult, nil
	}
	statusSetTime := time.Now().Format(time.RFC3339)
	return jc.updateJobStatus(ctx, uuid, queuedStatus, statusSetTime)
}

func (jc *JobController) CancelJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "status")
	if err != nil {
		return emptyResult, nil
	}
	if status == canceledStatus || status == failedStatus || status == completedStatus {
		emptyResult.Info = fmt.Sprintf(" The job status is %s and can't canceld", status)
		return emptyResult, nil
	}
	statusSetTime := time.Now().Format(time.RFC3339)
	qr, err := jc.updateJobStatus(ctx, uuid, canceledStatus, statusSetTime)
	if err != nil {
		return emptyResult, nil
	}

	tableName, _ := jc.GetStrJobInfo(ctx, uuid, "table_name")

	// 相比于pause，cancel需要删除内存中的元数据
	jc.deleteDMLJobRunningMeta(uuid, tableName)

	jc.notifyJobScheduler()

	return qr, nil
}

// 指定throttle的时长和ratio
// ratio表示限流的比例，最大为1，即完全限流
// 时长的格式举例：
// "300ms" 表示 300 毫秒。
// "-1.5h" 表示负1.5小时。
// "2h45m" 表示2小时45分钟。
func (jc *JobController) ThrottleJob(uuid, expireString string, ratioLiteral *sqlparser.Literal) (result *sqltypes.Result, err error) {
	emptyResult := &sqltypes.Result{}
	duration, ratio, err := jc.validateThrottleParams(expireString, ratioLiteral)
	if err != nil {
		return nil, err
	}
	if err := jc.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	expireAt := time.Now().Add(duration)
	_ = jc.lagThrottler.ThrottleApp(uuid, expireAt, ratio)

	query, err := sqlparser.ParseAndBind(sqlDMLJobUpdateThrottleInfo,
		sqltypes.Float64BindVariable(ratio),
		sqltypes.StringBindVariable(expireAt.String()),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return emptyResult, err
	}
	ctx := context.Background()
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	return jc.execQuery(ctx, "", query)
}

func (jc *JobController) UnthrottleJob(uuid string) (result *sqltypes.Result, err error) {
	emptyResult := &sqltypes.Result{}
	if err := jc.lagThrottler.CheckIsReady(); err != nil {
		return nil, err
	}
	_ = jc.lagThrottler.UnthrottleApp(uuid)

	query, err := sqlparser.ParseAndBind(sqlDMLJobClearThrottleInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return emptyResult, err
	}
	ctx := context.Background()
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	return jc.execQuery(ctx, "", query)
}

var throttleTicks int64
var throttleInit sync.Once

func initThrottleTicker() {
	throttleInit.Do(func() {
		go func() {
			tick := time.NewTicker(throttleCheckInterval)
			defer tick.Stop()
			for range tick.C {
				atomic.AddInt64(&throttleTicks, 1)
			}
		}()
	})
}

func (jc *JobController) requestThrottle(uuid string) (throttleCheckOK bool) {
	if jc.lastSuccessfulThrottle >= atomic.LoadInt64(&throttleTicks) {
		// if last check was OK just very recently there is no need to check again
		return true
	}
	ctx := context.Background()
	// 请求时给每一个throttle的app名都加上了dml-job前缀，这样可以通过throttle dml-job来throttle所有的dml jobs
	appName := "dml-job:" + uuid
	// 这里不特别设置flag
	throttleCheckFlags := &throttle.CheckFlags{}
	// 由于dml job子任务需要同步到集群中的各个从节点，因此throttle也依据的是集群的复制延迟
	checkType := throttle.ThrottleCheckPrimaryWrite
	checkRst := jc.lagThrottler.CheckByType(ctx, appName, "", throttleCheckFlags, checkType)
	if checkRst.StatusCode != http.StatusOK {
		return false
	}
	jc.lastSuccessfulThrottle = atomic.LoadInt64(&throttleTicks)
	return true
}

func (jc *JobController) validateThrottleParams(expireString string, ratioLiteral *sqlparser.Literal) (duration time.Duration, ratio float64, err error) {
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

	jc.deleteDMLJobRunningMeta(uuid, tableName)
	jc.notifyJobScheduler()
}

func (jc *JobController) jobScheduler(checkBeforeSchedule chan struct{}) {
	// 等待healthcare扫一遍后再进行

	<-checkBeforeSchedule
	ctx := context.Background()
	timer := time.NewTicker(time.Duration(jobSchedulerRunningInterval))
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-jc.schedulerNotifyChan:
		}

		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()
		// 先提交的job先执行

		qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetJobsToSchedule)
		if qr != nil {
			for _, row := range qr.Named().Rows {
				status := row["status"].ToString()
				schema := row["table_schema"].ToString()
				table := row["table_name"].ToString()
				uuid := row["job_uuid"].ToString()
				jobBatchTable := row["batch_info_table_name"].ToString()
				batchInterval, _ := row["batch_interval_in_ms"].ToInt64()
				batchSize, _ := row["batch_size"].ToInt64()
				runningTimePeriodStart := row["running_time_period_start"].ToString()
				runningTimePeriodEnd := row["running_time_period_end"].ToString()
				periodStartTimePtr, periodEndTimePtr := getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd)
				failPolicy := row["fail_policy"].ToString()

				if jc.checkDmlJobRunnable(uuid, status, table, periodStartTimePtr, periodEndTimePtr) {
					// 初始化Job在内存中的元数据，防止在dmlJobBatchRunner修改表中的状态前，scheduler多次启动同一个job
					jc.initDMLJobRunningMeta(uuid, table)
					go jc.dmlJobBatchRunner(uuid, table, schema, jobBatchTable, failPolicy, batchInterval, batchSize, periodStartTimePtr, periodEndTimePtr)
				}
			}
		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
	}
}

func getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd string) (*time.Time, *time.Time) {
	if runningTimePeriodStart != "" && runningTimePeriodEnd != "" {
		// 在submit job时或setRunningTimePeriod时，已经对格式进行了检查，因此这里不会出现错误
		periodStartTime, _ := time.Parse(time.TimeOnly, runningTimePeriodStart)
		periodEndTime, _ := time.Parse(time.TimeOnly, runningTimePeriodEnd)
		// 由于用户只提供了时间部分，因此需要将日期部分用当天的时间补齐。
		currentTime := time.Now()
		periodStartTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), periodStartTime.Hour(), periodStartTime.Minute(), periodStartTime.Second(), periodStartTime.Nanosecond(), currentTime.Location())
		periodEndTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), periodEndTime.Hour(), periodEndTime.Minute(), periodEndTime.Second(), periodEndTime.Nanosecond(), currentTime.Location())
		// 如果EndTime早于startTime的时间，则EndTime的日期部分用明天的日期补齐
		if periodEndTime.Before(periodStartTime) {
			periodEndTime = periodEndTime.Add(24 * time.Hour)
		}
		return &periodStartTime, &periodEndTime
	}
	return nil, nil
}

// todo，可以增加并发Job数的限制
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
			_, _ = jc.execQuery(context.Background(), "", submitQuery)
			return false
		}
	}

	return true
}

func (jc *JobController) getBatchIDToExec(ctx context.Context, batchTableSchema, batchTableName string) (string, error) {
	getBatchIDToExecSQL := fmt.Sprintf(sqlTemplateGetBatchIDToExec, batchTableName)
	qr, err := jc.execQuery(ctx, batchTableSchema, getBatchIDToExecSQL)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", nil
	}
	return qr.Named().Rows[0].ToString("batch_id")
}

func (jc *JobController) updateDealingBatchID(ctx context.Context, uuid string, batchID string) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlUpdateDealingBatchID,
		sqltypes.StringBindVariable(batchID),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return err
	}
	return nil
}

// todo，由于目前尚不支持一个job的batch并行，因此对job的batch表进行访问不用设锁
func (jc *JobController) getBatchSQLsByID(ctx context.Context, batchID, batchTableName, tableSchema string) (batchSQL, batchCountSQL string, err error) {
	getBatchSQLWithTableName := fmt.Sprintf(sqlTemplateGetBatchSQLsByID, batchTableName)
	query, err := sqlparser.ParseAndBind(getBatchSQLWithTableName,
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return "", "", err
	}
	qr, err := jc.execQuery(ctx, tableSchema, query)
	if err != nil {
		return "", "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", "", errors.New("the len of qr of getting batch sql by ID is not 1")
	}
	batchSQL, _ = qr.Named().Rows[0].ToString("batch_sql")
	batchCountSQL, _ = qr.Named().Rows[0].ToString("batch_count_sql_when_creating_batch")
	return batchSQL, batchCountSQL, nil
}

func (jc *JobController) getMaxBatchID(ctx context.Context, batchTableName, tableSchema string) (string, error) {
	getMaxBatchIDWithTableName := fmt.Sprintf(sqlTemplateGetMaxBatchID, batchTableName)
	qr, err := jc.execQuery(ctx, tableSchema, getMaxBatchIDWithTableName)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", errors.New("the len of qr of getting batch sql by ID is not 1")
	}
	return qr.Named().Rows[0].ToString("max_batch_id")
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
	// batchSize = 30
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
	// 1.根据batchCountSQL生成查询pk值的select sql
	// 1.1.获得PK信息
	pkInfos, err := jc.getTablePkInfo(ctx, tableSchema, table)
	if err != nil {
		return "", err
	}

	// 1.2.根据当前batch的batchCountSQL生成select sql，用于获得拆分后batch的拆分列start和end
	// 只需要将batchCountSQL的投影部分(SelectExprs)从count(*)改为拆分列即可
	batchCountSQLStmt, err := sqlparser.Parse(batchCountSQL)
	if err != nil {
		return "", err
	}
	batchCountSQLStmtSelect, _ := batchCountSQLStmt.(*sqlparser.Select)
	var pkExprs []sqlparser.SelectExpr
	for _, pkInfo := range pkInfos {
		pkExprs = append(pkExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName(pkInfo.pkName)})
	}
	batchCountSQLStmtSelect.SelectExprs = pkExprs
	batchSplitSelectSQL := sqlparser.String(batchCountSQLStmtSelect)

	// 2.根据select sql将batch拆分，生成两个新的batch。
	//这里每次只将超过threshold的batch拆成两个batch而不是多个小于等于threshold的batch的原因是：
	// 拆成多个batch需要遍历完select的全部结果，这可能会导致超时

	// 2.1.计算两个batch的batchPKStart和batchPKEnd。实际上，只要获得当前batch的新的PKEnd和新的batch的PKStart

	// 遍历前threshold+1条，依然使用同一个连接
	qr, err := conn.Exec(ctx, batchSplitSelectSQL, math.MaxInt32, true)
	if err != nil {
		return "", err
	}

	var curBatchNewEnd []any
	var newBatchStart []any

	for rowCount, row := range qr.Named().Rows {
		// 将原本batch的PKEnd设在threshold条数处
		if int64(rowCount) == batchSize-1 {
			for _, pkInfo := range pkInfos {
				pkName := pkInfo.pkName
				keyVal, err := ProcessValue(row[pkName])
				if err != nil {
					return "", err
				}
				curBatchNewEnd = append(curBatchNewEnd, keyVal)
			}
		}
		// 将第threshold+1条的PK作为新PK的起点
		if int64(rowCount) == batchSize {
			for _, pkInfo := range pkInfos {
				pkName := pkInfo.pkName
				keyVal, err := ProcessValue(row[pkName])
				if err != nil {
					return "", err
				}
				newBatchStart = append(newBatchStart, keyVal)
			}
		}
	}

	// 2.2) 将curBatchNewEnd和newBatchStart转换成sql中where部分的<=和>=的字符串
	curBatchLessThanPart, err := genPKsLessThanPart(pkInfos, curBatchNewEnd)
	if err != nil {
		return "", err
	}

	newBatchGreatThanPart, err := genPKsGreaterThanPart(pkInfos, newBatchStart)
	if err != nil {
		return "", err
	}

	// 2.3) 通过正则表达式，获得原先batchSQL中的great than和less than部分，作为当前batch的great than和新batch的less than部分
	// 定义正则表达式，匹配"( (greatThanPart) AND (lessThanPart) )"
	curBatchGreatThanPart := ""
	newBatchLessThanPart := ""

	// 这个正则表达式用于匹配出"( (greatThanPart) AND (lessThanPart) )"greatThanPart和lessThanPart，也就是每条batch sql中用于限定PK范围的部分
	regexPattern := `\(\s*\((.*)\)\s*AND\s*\((.*)\)\s*\)`

	// 编译正则表达式
	regex := regexp.MustCompile(regexPattern)

	// 查找匹配项
	matches := regex.FindAllStringSubmatch(batchSQL, -1)

	// 如果有匹配项，只取最后一个匹配的结果，因为用户自己输入的where条件中也可能存在这样的格式
	pkConditionPart := ""
	if len(matches) > 0 {
		lastMatch := matches[len(matches)-1]
		if len(lastMatch) == 3 {
			pkConditionPart = lastMatch[0]
			curBatchGreatThanPart = lastMatch[1]
			newBatchLessThanPart = lastMatch[2]
		}
	} else {
		return "", errors.New("can not match greatThan and lessThan parts by regex")
	}

	// 2.4) 生成拆分后，当前batch的sql和新batch的sql
	batchSQLCommonPart := strings.Replace(batchSQL, pkConditionPart, "", 1)
	batchCountSQLCommonPart := strings.Replace(batchCountSQL, pkConditionPart, "", 1)
	curBatchSQL := batchSQLCommonPart + fmt.Sprintf("( (%s) AND (%s) )", curBatchGreatThanPart, curBatchLessThanPart)
	newBatchSQL := batchSQLCommonPart + fmt.Sprintf("( (%s) AND (%s) )", newBatchGreatThanPart, newBatchLessThanPart)
	newBatchCountSQL := batchCountSQLCommonPart + fmt.Sprintf("( (%s) AND (%s) )", newBatchGreatThanPart, newBatchLessThanPart)
	// 构建当前batch新的batch begin及end字段以及新batch的begin及end字段
	getBatchBeginAndEndSQL := fmt.Sprintf(sqlTemplateGetBatchBeginAndEnd, batchTable)
	getBatchBeginAndEndQuery, err := sqlparser.ParseAndBind(getBatchBeginAndEndSQL, sqltypes.StringBindVariable(batchID))
	if err != nil {
		return "", err
	}
	qr, err = conn.Exec(ctx, getBatchBeginAndEndQuery, math.MaxInt32, true)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", errors.New("can not get batch begin and end")
	}
	currentBatchNewBeginStr := qr.Named().Rows[0]["batch_begin"].ToString()
	newBatchEndStr := qr.Named().Rows[0]["batch_end"].ToString()
	currentBatchNewEndStr, newBatchBegintStr, err := genBatchStartAndEndStr(curBatchNewEnd, newBatchStart, pkInfos)
	if err != nil {
		return "", err
	}

	// 2.5) 在batch表中更改旧的条目的sql，并插入新batch条目
	// 在表中更改旧的sql
	updateBatchSQL := fmt.Sprintf(sqlTemplateUpdateBatchSQL, batchTable)
	updateBatchSQLQuery, err := sqlparser.ParseAndBind(updateBatchSQL,
		sqltypes.StringBindVariable(curBatchSQL),
		sqltypes.StringBindVariable(currentBatchNewBeginStr),
		sqltypes.StringBindVariable(currentBatchNewEndStr),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return "", err
	}
	_, err = conn.Exec(ctx, updateBatchSQLQuery, math.MaxInt32, false)
	if err != nil {
		return "", err
	}
	// 插入新batch条目
	newCurrentBatchSQL = curBatchSQL
	nextBatchID, err := genNewBatchID(batchID)
	if err != nil {
		return "", err
	}
	newBatchSize := expectedRow - batchSize
	insertBatchSQL := fmt.Sprintf(sqlTemplateInsertBatchEntry, batchTable)
	insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQL,
		sqltypes.StringBindVariable(nextBatchID),
		sqltypes.StringBindVariable(newBatchSQL),
		sqltypes.StringBindVariable(newBatchCountSQL),
		sqltypes.Int64BindVariable(newBatchSize),
		sqltypes.StringBindVariable(newBatchBegintStr),
		sqltypes.StringBindVariable(newBatchEndStr))
	if err != nil {
		return "", err
	}
	_, err = conn.Exec(ctx, insertBatchSQLQuery, math.MaxInt32, false)
	if err != nil {
		return "", err
	}
	return newCurrentBatchSQL, nil
}

func (jc *JobController) dmlJobBatchRunner(uuid, table, tableSchema, batchTable, failPolicy string, batchInterval, batchSize int64, timePeriodStart, timePeriodEnd *time.Time) {

	// batchInterval 单位ms，duration输入ns，应该乘上1000000
	timer := time.NewTicker(time.Duration(batchInterval * 1e6))
	defer timer.Stop()

	ctx := context.Background()

	_, err := jc.updateJobStatus(ctx, uuid, runningStatus, time.Now().Format(time.RFC3339))
	if err != nil {
		jc.FailJob(ctx, uuid, err.Error(), table)
	}

	// 在一个无限循环中等待定时器触发
	for range timer.C {
		// 定时器触发时执行的函数
		// 检查状态是否为running，可能为paused/canceled
		status, err := jc.GetStrJobInfo(ctx, uuid, "status")
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
		if status != runningStatus {
			return
		}

		// 检查是否在运维窗口内
		if timePeriodStart != nil && timePeriodEnd != nil {
			currentTime := time.Now()
			if !(currentTime.After(*timePeriodStart) && currentTime.Before(*timePeriodEnd)) {
				_, err = jc.updateJobStatus(ctx, uuid, notInTimePeriodStatus, currentTime.Format(time.RFC3339))
				if err != nil {
					jc.FailJob(ctx, uuid, err.Error(), table)
				}
				return
			}
		}

		// 先请求throttle，若被throttle阻塞，则等待下一次timer事件
		if !jc.requestThrottle(uuid) {
			continue
		}

		// 获取本次要执行的batch的batchId
		batchIDToExec, err := jc.getBatchIDToExec(ctx, tableSchema, batchTable)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
		if batchIDToExec == "" {
			// 意味着所有的batch都已经执行完毕，则退出
			_, err = jc.CompleteJob(ctx, uuid, table)
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error(), table)
			}
			return
		}

		// 将batchID信息记录在系统表中便于用户查看
		err = jc.updateDealingBatchID(ctx, uuid, batchIDToExec)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}

		batchSQL, batchCountSQL, err := jc.getBatchSQLsByID(ctx, batchIDToExec, batchTable, tableSchema)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}

		// 执行当前batch的batch sql，并获得下一要执行的batchID
		err = jc.execBatchAndRecord(ctx, tableSchema, table, batchSQL, batchCountSQL, uuid, batchTable, batchIDToExec, batchSize)
		// 如果执行batch时失败，则根据failPolicy决定处理策略
		if err != nil {
			switch failPolicy {
			case failPolicyAbort:
				jc.FailJob(ctx, uuid, err.Error(), table)
				return
			case failPolicySkip:
				// todo，由于目前batch是串行执行，不存在多个协程同时访问batch表的情况，因此暂时不用加锁。
				_ = jc.updateBatchStatus(tableSchema, batchTable, batchIDToExec, failPolicySkip, err.Error())
				continue
			case failPolicyPause:
				msg := fmt.Sprintf("batch %s failed, pause job: %s", batchIDToExec, err.Error())
				_ = jc.updateJobMessage(ctx, uuid, msg)
				_, _ = jc.updateJobStatus(ctx, uuid, pausedStatus, time.Now().Format(time.RFC3339))
				return
			}
		}
	}
}

// 调用该函数时，需要外部要获取相关的锁
func (jc *JobController) initDMLJobRunningMeta(uuid, table string) {
	jc.workingTables[table] = true
}

func (jc *JobController) deleteDMLJobRunningMeta(uuid, table string) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()
	delete(jc.workingTables, table)
}

// execQuery execute sql by using connect poll,so if targetString is not empty, it will add prefix `use database` first then execute sql.
func (jc *JobController) execQuery(ctx context.Context, targetString, query string) (result *sqltypes.Result, err error) {
	defer jc.env.LogError()
	var setting pools.Setting
	if targetString != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", targetString))
		setting.SetResetQuery(fmt.Sprintf("use %s", jc.env.Config().DB.DBName))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	if err != nil {
		return result, err
	}
	qr, err := conn.Exec(ctx, query, math.MaxInt32, true)
	conn.Recycle()
	return qr, err

}

func (jc *JobController) execSubtaskAndRecord(ctx context.Context, tableSchema, subtaskSQL, uuid string) (affectedRows int64, err error) {
	defer jc.env.LogError()

	var setting pools.Setting
	if tableSchema != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", tableSchema))
		setting.SetResetQuery(fmt.Sprintf("use %s", jc.env.Config().DB.DBName))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	defer conn.Recycle()
	if err != nil {
		return 0, err
	}

	_, err = conn.Exec(ctx, "start transaction", math.MaxInt32, false)
	if err != nil {
		return 0, err
	}

	qr, err := conn.Exec(ctx, subtaskSQL, math.MaxInt32, true)
	affectedRows = int64(qr.RowsAffected)

	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	recordRstSQL, err := sqlparser.ParseAndBind(sqlDMLJobUpdateAffectedRows,
		sqltypes.Int64BindVariable(affectedRows),
		sqltypes.StringBindVariable(uuid))
	_, err = conn.Exec(ctx, recordRstSQL, math.MaxInt32, false)
	if err != nil {
		return 0, err
	}
	_, err = conn.Exec(ctx, "commit", math.MaxInt32, false)
	if err != nil {
		return 0, err
	}

	return affectedRows, nil
}

// 该函数拿锁
func (jc *JobController) updateJobMessage(ctx context.Context, uuid, message string) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateMessage,
		sqltypes.StringBindVariable(message),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	return err
}

func (jc *JobController) updateJobAffectedRows(ctx context.Context, uuid string, affectedRows int64) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateAffectedRows,
		sqltypes.Int64BindVariable(affectedRows),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	return err
}

func (jc *JobController) updateJobStatus(ctx context.Context, uuid, status, statusSetTime string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
		sqltypes.StringBindVariable(status),
		sqltypes.StringBindVariable(statusSetTime),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) updateJobPeriodTime(ctx context.Context, uuid, timePeriodStart, timePeriodEnd string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateTimePeriod,
		sqltypes.StringBindVariable(timePeriodStart),
		sqltypes.StringBindVariable(timePeriodEnd),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) GetIntJobInfo(ctx context.Context, uuid, fieldName string) (int64, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobGetInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return 0, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, fmt.Errorf("uuid %s has %d entrys in the table instead of 1", uuid, len(qr.Named().Rows))
	}
	return qr.Named().Rows[0].ToInt64(fieldName)
}

func (jc *JobController) GetStrJobInfo(ctx context.Context, uuid, fieldName string) (string, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobGetInfo,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return "", err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) != 1 {
		return "", fmt.Errorf("uuid %s has %d entrys in the table instead of 1", uuid, len(qr.Named().Rows))
	}
	return qr.Named().Rows[0].ToString(fieldName)
}

func buildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: collations.CollationUtf8ID,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

func buildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

func (jc *JobController) getTablePkInfo(ctx context.Context, tableSchema, tableName string) ([]PKInfo, error) {
	// 1. 先获取pks 的名字
	submitQuery, err := sqlparser.ParseAndBind(sqlGetTablePk,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return nil, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return nil, err
	}
	var pkNames []string
	for _, row := range qr.Named().Rows {
		pkNames = append(pkNames, row["COLUMN_NAME"].ToString())
	}

	// 2. 根据获得的pk列的名字，去原表中查一行数据，借助封装好的Value对象获得每个pk的类型
	pkCols := ""
	firstPK := true
	for _, pkName := range pkNames {
		if !firstPK {
			pkCols += ","
		}
		pkCols += pkName
		firstPK = false
	}
	selectPKCols := fmt.Sprintf(sqlTemplateSelectPKCols, pkCols, tableSchema, tableName)
	qr, err = jc.execQuery(ctx, "", selectPKCols)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) != 1 {
		return nil, errors.New("the len of qr of select pk cols should be 1")
	}
	// 获得每一列的type，并生成pkInfo切片
	var pkInfos []PKInfo
	for _, pkName := range pkNames {
		pkInfos = append(pkInfos, PKInfo{pkName: pkName, pkType: qr.Named().Rows[0][pkName].Type()})
	}

	return pkInfos, nil
}

func (jc *JobController) getTableColNames(ctx context.Context, tableSchema, tableName string) ([]string, error) {
	submitQuery, err := sqlparser.ParseAndBind(sqlGetTableColNames,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return nil, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return nil, err
	}
	var colNames []string
	for _, row := range qr.Named().Rows {
		colNames = append(colNames, row["COLUMN_NAME"].ToString())
	}
	return colNames, nil
}

func (jc *JobController) jobHealthCheck(checkBeforeSchedule chan struct{}) {
	ctx := context.Background()

	// 用于crash后，重启时，先扫一遍running和paused的
	qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
	if qr != nil {

		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()

		for _, row := range qr.Named().Rows {
			status := row["status"].ToString()
			tableSchema := row["table_schema"].ToString()
			table := row["table_name"].ToString()
			jobBatchTable := row["batch_info_table_name"].ToString()
			uuid := row["job_uuid"].ToString()
			batchInterval, _ := row["batch_interval_in_ms"].ToInt64()
			batchSize, _ := row["batch_size"].ToInt64()
			runningTimePeriodStart := row["running_time_period_start"].ToString()
			runningTimePeriodEnd := row["running_time_period_end"].ToString()
			periodStartTimePtr, periodEndTimePtr := getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd)
			failPolicy := row["fail_policy"].ToString()

			if status == runningStatus {
				jc.initDMLJobRunningMeta(uuid, table)
				go jc.dmlJobBatchRunner(uuid, table, tableSchema, jobBatchTable, failPolicy, batchInterval, batchSize, periodStartTimePtr, periodEndTimePtr)
			}

			// 对于暂停的，不启动协程，只需要恢复内存元数据
			if status == pausedStatus {
				jc.initDMLJobRunningMeta(uuid, table)
			}

		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
	}

	log.Info("check of running and paused done \n")
	checkBeforeSchedule <- struct{}{}

	timer := time.NewTicker(healthCheckInterval)
	defer timer.Stop()

	for range timer.C {
		// todo, 增加对长时间未增加 rows的处理
		jc.tableMutex.Lock()
		qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
		if qr != nil {
			for _, row := range qr.Named().Rows {
				status := row["status"].ToString()
				statusSetTime := row["status_set_time"].ToString()
				uuid := row["job_uuid"].ToString()
				jobBatchTable := row["batch_info_table_name"].ToString()
				tableSchema := row["table_schema"].ToString()

				statusSetTimeObj, err := time.Parse(time.RFC3339, statusSetTime)
				if err != nil {
					continue
				}

				if status == canceledStatus || status == failedStatus || status == completedStatus {
					if time.Now().After(statusSetTimeObj.Add(tableEntryGCInterval)) {
						deleteJobSQL, err := sqlparser.ParseAndBind(sqlDMLJobDeleteJob,
							sqltypes.StringBindVariable(uuid))
						if err != nil {
							continue
						}
						_, _ = jc.execQuery(ctx, "", deleteJobSQL)

						_, _ = jc.execQuery(ctx, tableSchema, fmt.Sprintf(sqlTemplateDropTable, jobBatchTable))
					}
				}
			}
		}

		jc.tableMutex.Unlock()
	}
}

func (jc *JobController) createJobBatches(jobUUID, sql, tableSchema string, userBatchSize int64) (tableName, batchTableName string, batchSize int64, err error) {
	// 1.解析用户提交的DML sql，返回DML的各个部分。其中selectSQL用于确定每一个batch的pk范围，生成每一个batch所要执行的batch sql
	selectSQL, tableName, wherePart, pkPart, whereExpr, pkInfos, stmt, err := jc.parseDML(sql, tableSchema)
	if err != nil {
		return "", "", 0, err
	}

	// 2.利用selectSQL为该job生成batch表，在此之前生成每个batch的batchSize
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
	batchTableName, err = jc.createBatchTable(jobUUID, selectSQL, tableSchema, sql, tableName, wherePart, pkPart, whereExpr, stmt, pkInfos, batchSize)
	return tableName, batchTableName, batchSize, err
}

func (jc *JobController) parseDML(sql, tableSchema string) (selectSQL, tableName, wherePart, pkPart string, whereExpr sqlparser.Expr, pkInfos []PKInfo, stmt sqlparser.Statement, err error) {
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		return "", "", "", "", nil, nil, nil, err
	}
	// 根据stmt，分析DML SQL的各个部分，包括涉及的表，where条件
	switch s := stmt.(type) {
	case *sqlparser.Delete:
		if len(s.TableExprs) != 1 {
			return "", "", "", "", nil, nil, nil, errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		// todo 目前暂不支持join和多表
		if !ok {
			return "", "", "", "", nil, nil, nil, errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		wherePart = sqlparser.String(s.Where)
		if wherePart == "" {
			return "", "", "", "", nil, nil, nil, errors.New("the sql doesn't have where condition")
		}
		// 将where字符串中的"where"字符串删除，便于对真正的条件部分增加括号
		wherePart = wherePart[strings.Index(wherePart, "where")+5:]
		whereExpr = s.Where.Expr

		limitPart := sqlparser.String(s.Limit)
		if limitPart != "" {
			return "", "", "", "", nil, nil, nil, errors.New("the SQL should not have limit clause")
		}
		orderByPart := sqlparser.String(s.OrderBy)
		if orderByPart != "" {
			return "", "", "", "", nil, nil, nil, errors.New("the SQL should not have order by clause")
		}

	case *sqlparser.Update:
		if len(s.TableExprs) != 1 {
			return "", "", "", "", nil, nil, nil, errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		// todo 目前暂不支持join和多表
		if !ok {
			return "", "", "", "", nil, nil, nil, errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		wherePart = sqlparser.String(s.Where)
		if wherePart == "" {
			return "", "", "", "", nil, nil, nil, errors.New("the sql doesn't have where condition")
		}
		// 将where字符串中的"where"字符串删除，便于对真正的条件部分增加括号
		wherePart = wherePart[strings.Index(wherePart, "where")+5:]
		whereExpr = s.Where.Expr

		limitPart := sqlparser.String(s.Limit)
		if limitPart != "" {
			return "", "", "", "", nil, nil, nil, errors.New("the SQL should not have limit clause")
		}
		orderByPart := sqlparser.String(s.OrderBy)
		if orderByPart != "" {
			return "", "", "", "", nil, nil, nil, errors.New("the SQL should not have order by clause")
		}

	default:
		// todo support select...into, replace...into
		return "", "", "", "", nil, nil, nil, errors.New("the type of sql is not supported")
	}

	// 获得该DML所相关表的PK信息，将其中的PK列组成字符串pkPart，形如"PKCol1,PKCol2,PKCol3"
	ctx := context.Background()
	pkInfos, err = jc.getTablePkInfo(ctx, tableSchema, tableName)
	if err != nil {
		return "", "", "", "", nil, nil, nil, err
	}
	pkPart = ""
	firstPK := true
	for _, pkInfo := range pkInfos {
		if !firstPK {
			pkPart += ","
		}
		pkPart += pkInfo.pkName
		firstPK = false
	}

	// 将该DML的各部分信息组成batch select语句，用于生成每一个batch的pk范围
	selectSQL = fmt.Sprintf("select %s from %s.%s where %s order by %s",
		pkPart, tableSchema, tableName, wherePart, pkPart)

	return selectSQL, tableName, wherePart, pkPart, whereExpr, pkInfos, stmt, err
}

func (jc *JobController) createBatchTable(jobUUID, selectSQL, tableSchema, sql, tableName, wherePart, pkPart string, whereExpr sqlparser.Expr, stmt sqlparser.Statement, pkInfos []PKInfo, batchSize int64) (string, error) {
	ctx := context.Background()

	// 执行selectSQL，获得有序的pk值结果集，以生成每一个batch要执行的batch SQL
	qr, err := jc.execQuery(ctx, "", selectSQL)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) == 0 {
		return "", nil
	}

	// 为每一个DML job创建一张batch表，保存着该job被拆分成batches的具体信息。
	// healthCheck协程会定时对处于结束状态(completed,canceled,failed)的job的batch表进行回收
	batchTableName := "batch_info_table_" + strings.Replace(jobUUID, "-", "_", -1)

	createTableSQL := fmt.Sprintf(sqlTemplateCreateBatchTable, batchTableName)
	_, err = jc.execQuery(ctx, tableSchema, createTableSQL)
	if err != nil {
		return "", err
	}

	// 遍历每一行的每一个PK的值，记录每一个batch的开始和结束pk值（当有多个pk列时，需要记录多个pk值，pk可能具有不同的数据类型
	// 当遍历的行数达到一个batchSize时，即可生成一个batch所要执行的batch SQL，往batch表中插入一个条目
	currentBatchSize := int64(0)
	var currentBatchStart []any
	var currentBatchEnd []any
	currentBatchID := "1"
	insertBatchSQLWithTableName := fmt.Sprintf(sqlTemplateInsertBatchEntry, batchTableName)

	for _, row := range qr.Named().Rows {
		var pkValues []any
		for _, pkInfo := range pkInfos {
			pkName := pkInfo.pkName
			keyVal, err := ProcessValue(row[pkName])
			pkValues = append(pkValues, keyVal)
			if err != nil {
				return "", err
			}
		}
		if currentBatchSize == 0 {
			currentBatchStart = pkValues
		}
		currentBatchEnd = pkValues
		currentBatchSize++
		if currentBatchSize == batchSize {
			batchSQL, err := jc.genBatchSQL(sql, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
			if err != nil {
				return "", err
			}
			countSQL, err := genCountSQL(tableSchema, tableName, wherePart, pkPart, currentBatchStart, currentBatchEnd, pkInfos)
			if err != nil {
				return "", err
			}
			batchStartStr, batchEndStr, err := genBatchStartAndEndStr(currentBatchStart, currentBatchEnd, pkInfos)
			if err != nil {
				return "", err
			}
			currentBatchSize = 0
			insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQLWithTableName,
				sqltypes.StringBindVariable(currentBatchID),
				sqltypes.StringBindVariable(batchSQL),
				sqltypes.StringBindVariable(countSQL),
				sqltypes.Int64BindVariable(batchSize),
				sqltypes.StringBindVariable(batchStartStr),
				sqltypes.StringBindVariable(batchEndStr))
			if err != nil {
				return "", err
			}
			_, err = jc.execQuery(ctx, tableSchema, insertBatchSQLQuery)
			if err != nil {
				return "", err
			}
			currentBatchID, err = currentBatchIDInc(currentBatchID)
			if err != nil {
				return "", err
			}
		}
	}
	// 最后一个batch的行数不一定是batchSize，在循环结束时要将剩余的行数划分到最后一个batch中
	if currentBatchSize != 0 {
		batchSQL, err := jc.genBatchSQL(sql, stmt, whereExpr, currentBatchStart, currentBatchEnd, pkInfos)
		if err != nil {
			return "", err
		}
		countSQL, err := genCountSQL(tableSchema, tableName, wherePart, pkPart, currentBatchStart, currentBatchEnd, pkInfos)
		if err != nil {
			return "", err
		}
		batchStartStr, batchEndStr, err := genBatchStartAndEndStr(currentBatchStart, currentBatchEnd, pkInfos)
		if err != nil {
			return "", err
		}
		insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQLWithTableName,
			sqltypes.StringBindVariable(currentBatchID),
			sqltypes.StringBindVariable(batchSQL),
			sqltypes.StringBindVariable(countSQL),
			sqltypes.Int64BindVariable(currentBatchSize),
			sqltypes.StringBindVariable(batchStartStr),
			sqltypes.StringBindVariable(batchEndStr))
		if err != nil {
			return "", err
		}
		_, err = jc.execQuery(ctx, tableSchema, insertBatchSQLQuery)
		if err != nil {
			return "", err
		}
	}
	return batchTableName, nil
}

func currentBatchIDInc(currentBatchID string) (string, error) {
	if strings.Contains(currentBatchID, "-") {
		currentBatchID = strings.Split(currentBatchID, "-")[0]
	}
	currentBatchIDInt64, err := strconv.ParseInt(currentBatchID, 10, 64)
	if err != nil {
		return "", err
	}
	currentBatchIDInt64++
	return strconv.FormatInt(currentBatchIDInt64, 10), nil
}

func genBatchStartAndEndStr(currentBatchStart, currentBatchEnd []any, pkInfos []PKInfo) (currentBatchStartStr string, currentBatchStartEnd string, err error) {
	for i, pkInfo := range pkInfos {
		placeHolder, err := genPlaceholderByType(pkInfo.pkType)
		if err != nil {
			return "", "", err
		}
		if i > 0 {
			currentBatchStartStr += ","
			currentBatchStartEnd += ","
		}
		currentBatchStartStr += placeHolder
		currentBatchStartEnd += placeHolder
		currentBatchStartStr = fmt.Sprintf(currentBatchStartStr, currentBatchStart[i])
		currentBatchStartEnd = fmt.Sprintf(currentBatchStartEnd, currentBatchEnd[i])
	}
	return currentBatchStartStr, currentBatchStartEnd, nil
}

// 拆分列所支持的类型需要满足以下条件：
// 1.在sql中可以正确地使用between或>=,<=进行比较运算，且没有精度问题。
// 2.可以转换成go中的int64，float64或string三种类型之一，且转换后，在golang中的比较规则和mysql中的比较规则相同
func genCountSQL(tableSchema, tableName, wherePart, pkPart string, currentBatchStart, currentBatchEnd []any, pkInfos []PKInfo) (countSQLTemplate string, err error) {
	if len(pkInfos) == 0 {
		return "", errors.New("the len of pkInfos is 0")
	}
	if len(pkInfos) == 1 {
		switch pkInfos[0].pkType {
		case querypb.Type_INT8, querypb.Type_INT16, querypb.Type_INT24, querypb.Type_INT32, querypb.Type_INT64:
			countSQLTemplate = fmt.Sprintf("select count(*) as count_rows from %s.%s where ( %s ) and ( %s between %d AND %d ) order by %s",
				tableSchema, tableName, wherePart, pkPart, currentBatchStart[0], currentBatchEnd[0], pkPart)

		case querypb.Type_UINT8, querypb.Type_UINT16, querypb.Type_UINT24, querypb.Type_UINT32, querypb.Type_UINT64:
			countSQLTemplate = fmt.Sprintf("select count(*) as count_rows from %s.%s where ( %s ) and ( %s between %d AND %d ) order by %s",
				tableSchema, tableName, wherePart, pkPart, currentBatchStart[0], currentBatchEnd[0], pkPart)

		case querypb.Type_FLOAT32, querypb.Type_FLOAT64:
			return "", errors.New("float type is unsupported")

		case querypb.Type_TIMESTAMP, querypb.Type_DATE, querypb.Type_TIME, querypb.Type_DATETIME, querypb.Type_YEAR,
			querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR:
			countSQLTemplate = fmt.Sprintf("select count(*) as count_rows from %s.%s where ( %s ) and ( %s between '%s' AND '%s' ) order by %s",
				tableSchema, tableName, wherePart, pkPart, currentBatchStart[0], currentBatchEnd[0], pkPart)

		default:
			return "", fmt.Errorf("Unsupported type: %v", pkInfos[0].pkType)
		}
	} else {
		// 1. 生成>=的部分
		// 遍历PKName，不同的pk类型要对应不同的占位符
		greatThanPart, err := genPKsGreaterThanPart(pkInfos, currentBatchStart)
		if err != nil {
			return "", err
		}

		// 2.生成<=的部分
		lessThanPart, err := genPKsLessThanPart(pkInfos, currentBatchEnd)
		if err != nil {
			return "", err
		}

		// 3.将各部分拼接成最终的template
		countSQLTemplate = fmt.Sprintf("select count(*) as count_rows from %s.%s where ( %s ) and ( (%s) AND (%s) )",
			tableSchema, tableName, wherePart, greatThanPart, lessThanPart)
	}
	return countSQLTemplate, nil
}

func genPlaceholderByType(typ querypb.Type) (string, error) {
	switch typ {
	case querypb.Type_INT8, querypb.Type_INT16, querypb.Type_INT24, querypb.Type_INT32, querypb.Type_INT64:
		return "%d", nil
	case querypb.Type_UINT8, querypb.Type_UINT16, querypb.Type_UINT24, querypb.Type_UINT32, querypb.Type_UINT64:
		return "%d", nil
	case querypb.Type_FLOAT32, querypb.Type_FLOAT64:
		return "%f", nil
	case querypb.Type_TIMESTAMP, querypb.Type_DATE, querypb.Type_TIME, querypb.Type_DATETIME, querypb.Type_YEAR,
		querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR:
		return "%s", nil
	default:
		return "", fmt.Errorf("Unsupported type: %v", typ)
	}
}

func ProcessValue(value sqltypes.Value) (any, error) {
	typ := value.Type()

	switch typ {
	case querypb.Type_INT8, querypb.Type_INT16, querypb.Type_INT24, querypb.Type_INT32, querypb.Type_INT64:
		return value.ToInt64()
	case querypb.Type_UINT8, querypb.Type_UINT16, querypb.Type_UINT24, querypb.Type_UINT32, querypb.Type_UINT64:
		return value.ToUint64()
	case querypb.Type_FLOAT32, querypb.Type_FLOAT64:
		return value.ToFloat64()
	case querypb.Type_TIMESTAMP, querypb.Type_DATE, querypb.Type_TIME, querypb.Type_DATETIME, querypb.Type_YEAR,
		querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR:
		return value.ToString(), nil
	default:
		return nil, fmt.Errorf("Unsupported type: %v", typ)
	}
}

func genPKsGreaterThanPart(pkInfos []PKInfo, currentBatchStart []any) (string, error) {
	curIdx := 0
	pksNum := len(pkInfos)
	var equalStr, rst string
	for curIdx < pksNum {
		curPkName := pkInfos[curIdx].pkName
		curPKType := pkInfos[curIdx].pkType
		// mysql的浮点类型在比较时有精度损失，不适合作为拆分列
		if curPKType == querypb.Type_FLOAT32 || curPKType == querypb.Type_FLOAT64 {
			return "", fmt.Errorf("unsupported type: %v", curPKType)
		}

		placeholder, err := genPlaceholderByType(curPKType)
		if err != nil {
			return "", err
		}

		if curIdx == 0 {
			rst = fmt.Sprintf("( %s > %s )", curPkName, placeholder)
		} else if curIdx != (pksNum - 1) {
			rst += fmt.Sprintf(" OR ( %s AND %s > %s )", equalStr, curPkName, placeholder)
		} else if curIdx == (pksNum - 1) {
			rst += fmt.Sprintf(" OR ( %s AND %s >= %s )", equalStr, curPkName, placeholder)
		}
		rst = fmt.Sprintf(rst, currentBatchStart[curIdx])

		if curIdx == 0 {
			equalStr = fmt.Sprintf("%s = %s", curPkName, placeholder)
		} else {
			equalStr += fmt.Sprintf(" AND %s = %s", curPkName, placeholder)
		}
		equalStr = fmt.Sprintf(equalStr, currentBatchStart[curIdx])
		curIdx++
	}
	return rst, nil
}

func genPKsLessThanPart(pkInfos []PKInfo, currentBatchEnd []any) (string, error) {
	curIdx := 0
	pksNum := len(pkInfos)
	var equalStr, rst string
	for curIdx < pksNum {
		curPkName := pkInfos[curIdx].pkName
		curPKType := pkInfos[curIdx].pkType
		// mysql的浮点类型在比较时有精度损失，不适合作为拆分列
		if curPKType == querypb.Type_FLOAT32 || curPKType == querypb.Type_FLOAT64 {
			return "", fmt.Errorf("unsupported type: %v", curPKType)
		}

		placeholder, err := genPlaceholderByType(curPKType)
		if err != nil {
			return "", err
		}

		if curIdx == 0 {
			rst = fmt.Sprintf("( %s < %s )", curPkName, placeholder)
		} else if curIdx != (pksNum - 1) {
			rst += fmt.Sprintf(" OR ( %s AND %s < %s )", equalStr, curPkName, placeholder)
		} else if curIdx == (pksNum - 1) {
			rst += fmt.Sprintf(" OR ( %s AND %s <= %s )", equalStr, curPkName, placeholder)
		}
		rst = fmt.Sprintf(rst, currentBatchEnd[curIdx])

		if curIdx == 0 {
			equalStr = fmt.Sprintf("%s = %s", curPkName, placeholder)
		} else {
			equalStr += fmt.Sprintf(" AND %s = %s", curPkName, placeholder)
		}
		equalStr = fmt.Sprintf(equalStr, currentBatchEnd[curIdx])
		curIdx++
	}
	return rst, nil
}

func (jc *JobController) genBatchSQL(sql string, stmt sqlparser.Statement, whereExpr sqlparser.Expr, currentBatchStart, currentBatchEnd []any, pkInfos []PKInfo) (batchSQL string, err error) {
	if len(pkInfos) == 1 {
		if fmt.Sprintf("%T", currentBatchStart[0]) != fmt.Sprintf("%T", currentBatchEnd[0]) {
			err = errors.New("the type of currentBatchStart and currentBatchEnd is different")
			return "", err
		}
		pkName := pkInfos[0].pkName
		switch currentBatchEnd[0].(type) {
		case int64:
			batchSQL = sql + fmt.Sprintf(" AND %s between %d AND %d", pkName, currentBatchStart[0].(int64), currentBatchEnd[0].(int64))
		case uint64:
			batchSQL = sql + fmt.Sprintf(" AND %s between %d AND %d", pkName, currentBatchStart[0].(uint64), currentBatchEnd[0].(uint64))
		case float64:
			return "", errors.New("float type is unsupported")
		case string:
			batchSQL = sql + fmt.Sprintf(" AND %s between '%s' AND '%s'", pkName, currentBatchStart[0].(string), currentBatchEnd[0].(string))
		default:
			err = errors.New("unsupported type of currentBatchEnd")
			return "", err
		}
	} else {
		// 1. 生成>=的部分
		// 遍历PKName，不同的pk类型要对应不同的占位符
		greatThanPart, err := genPKsGreaterThanPart(pkInfos, currentBatchStart)
		if err != nil {
			return "", err
		}

		// 2.生成<=的部分
		lessThanPart, err := genPKsLessThanPart(pkInfos, currentBatchEnd)
		if err != nil {
			return "", err
		}

		// 3.将pk>= and pk <= 拼接起来并生成相应的condition expr ast node
		pkConditionExpr, err := jc.genPKConditionExpr(greatThanPart, lessThanPart)
		if err != nil {
			return "", err
		}

		// 4.将原本sql stmt中的where expr ast node用AND拼接上pkConditionExpr，作为batchSQL的where expr ast node
		// 4.1先生成新的condition ast node
		andExpr := sqlparser.Where{Expr: &sqlparser.AndExpr{Left: whereExpr, Right: pkConditionExpr}}
		batchSQL = jc.genBatchSQLByReplaceWhereExprNode(stmt, andExpr)
		//batchSQL = sql + fmt.Sprintf(" AND ( (%s) AND (%s) )", greatThanPart, lessThanPart)
	}
	return batchSQL, nil
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

func (jc *JobController) getIndexCount(tableSchema, tableName string) (indexCount int64, err error) {
	query, err := sqlparser.ParseAndBind(sqlGetIndexCount,
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName))
	if err != nil {
		return 0, err
	}
	ctx := context.Background()
	qr, err := jc.execQuery(ctx, "", query)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, err
	}
	return qr.Named().Rows[0]["index_count"].ToInt64()
}

func genNewBatchID(batchID string) (newBatchID string, err error) {
	// 产生新的batchID
	if strings.Contains(batchID, "-") {
		parts := strings.Split(batchID, "-")
		num, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return "", err
		}
		newBatchID = fmt.Sprintf("%s-%d", parts[0], num+1)
	} else {
		num, err := strconv.ParseInt(batchID, 10, 64)
		if err != nil {
			return "", err
		}
		newBatchID = fmt.Sprintf("%d-1", num)
	}
	return newBatchID, nil
}

func (jc *JobController) updateBatchStatus(batchTableSchema, batchTableName, status, batchID, errStr string) (err error) {
	updateBatchStatusAndAffectedRowsSQL := fmt.Sprintf(sqlTempalteUpdateBatchStatusAndAffectedRows, batchTableName)
	query, err := sqlparser.ParseAndBind(updateBatchStatusAndAffectedRowsSQL,
		sqltypes.StringBindVariable(status+": "+errStr),
		sqltypes.Int64BindVariable(0),
		sqltypes.StringBindVariable(batchID))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(context.Background(), batchTableSchema, query)
	return err
}

func (jc *JobController) genPKConditionExpr(greatThanPart, lessThanPart string) (sqlparser.Expr, error) {
	tmpSQL := fmt.Sprintf("select 1 where (%s) AND (%s)", greatThanPart, lessThanPart)
	tmpStmt, err := sqlparser.Parse(tmpSQL)
	if err != nil {
		return nil, err
	}
	tmpStmtSelect, ok := tmpStmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("genPKConditionExpr: tmpStmt is not *sqlparser.Select")
	}
	return tmpStmtSelect.Where.Expr, nil
}

func (jc *JobController) genBatchSQLByReplaceWhereExprNode(stmt sqlparser.Statement, whereExpr sqlparser.Where) string {
	switch s := stmt.(type) {
	case *sqlparser.Update:
		s.Where = &whereExpr
		return sqlparser.String(s)
	case *sqlparser.Delete:
		s.Where = &whereExpr
		return sqlparser.String(s)
	default:
		// the code won't reach here
		return ""
	}
}
