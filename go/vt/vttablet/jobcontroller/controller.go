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

// todo newborn22, 数一下连接数是不是3够用
const (
	databasePoolSize   = 3
	healthCheckTimeGap = 10000 // ms
)

const (
	SubmitJob         = "submit_job"
	ShowJobs          = "show_jobs"
	LaunchJob         = "launch"
	LaunchAllJobs     = "launch_all"
	PauseJob          = "pause"
	PauseAllJobs      = "pause_all"
	ResumeJob         = "resume"
	ResumeAllJobs     = "resume_all"
	ThrottleJob       = "throttle"
	ThrottleAllJobs   = "throttle_all"
	UnthrottleJob     = "unthrottle"
	UnthrottleAllJobs = "unthrottle_all"
	CancelJob         = "cancel"
)

const (
	defaultTimeGap     = 1000 // 1000ms
	defaultSubtaskRows = 100
	defaultThreshold   = 3000 // todo，通过函数来计算出threshold并传入runner中，要依据索引的个数
)

const (
	postponeLaunchStatus = "postpone-launch"
	queuedStatus         = "queued"
	blockedStatus        = "blocked"
	runningStatus        = "running"
	pausedStatus         = "paused"
	interruptedStatus    = "interrupted"
	canceledStatus       = "canceled"
	failedStatus         = "failed"
	completedStatus      = "completed"
)

const (
	sqlDMLJobGetAllJobs = `select * from mysql.big_dml_jobs_table order by id;`
	sqlDMLJobSubmit     = `insert into mysql.big_dml_jobs_table (
                                      job_uuid,
                                      dml_sql,
                                      related_schema,
                                      related_table,
                                      job_batch_table,
                                      timegap_in_ms,
                                      subtask_rows,
                                      job_status) values(%a,%a,%a,%a,%a,%a,%a,%a)`

	sqlDMLJobUpdateMessage = `update mysql.big_dml_jobs_table set 
                                    message = %a 
                                where 
                                    job_uuid = %a`

	sqlDMLJobUpdateAffectedRows = `update mysql.big_dml_jobs_table set 
                                    affected_rows = affected_rows + %a 
                                where 
                                    job_uuid = %a`

	sqlDMLJobUpdateStatus = `update mysql.big_dml_jobs_table set 
                                    job_status = %a 
                                where 
                                    job_uuid = %a`

	sqlDMLJobGetInfo = `select * from mysql.big_dml_jobs_table 
                                where
                                	job_uuid = %a`

	sqlGetTablePk = ` SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
								WHERE 
						    		TABLE_SCHEMA = %a
									AND TABLE_NAME = %a
									AND CONSTRAINT_NAME = 'PRIMARY'`

	sqlGetTableColNames = `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
								WHERE 
								    TABLE_SCHEMA = %a
									AND TABLE_NAME = %a`

	sqlDMLJobUpdateThrottleInfo = `update mysql.big_dml_jobs_table set 
                                    throttle_ratio = %a ,
                                    throttle_expire_time = %a
                                where 
                                    job_uuid = %a`

	sqlDMLJobClearThrottleInfo = `update mysql.big_dml_jobs_table set 
                                    throttle_ratio = NULL ,
                                    throttle_expire_time = NULL
                                where 
                                    job_uuid = %a`
)

const (
	throttleCheckDuration = 250 * time.Millisecond
)

type JobController struct {
	tableName              string
	tableMutex             sync.Mutex // todo newborn22,检查是否都上锁了
	tabletTypeFunc         func() topodatapb.TabletType
	env                    tabletenv.Env
	pool                   *connpool.Pool
	lagThrottler           *throttle.Throttler
	lastSuccessfulThrottle int64

	workingTables      map[string]bool // 用于调度时检测当前任务是否和正在工作的表冲突，paused、running状态的job的表都在里面
	workingTablesMutex sync.Mutex

	// 当running或者paused的时候，应该在working uuid中，以此来做健康检测
	workingUUIDs      map[string]bool
	workingUUIDsMutex sync.Mutex

	jobChans            map[string]JobChanStruct
	jobChansMutex       sync.Mutex
	checkBeforeSchedule chan struct{}
}

type JobChanStruct struct {
	pauseAndResume chan string
	cancel         chan string
}

// todo newborn22, 初始化函数
// 要加锁？
func (jc *JobController) Open() error {
	// todo newborn22 ，改成英文注释
	// 只在primary上运行，记得在rpc那里也做处理
	// todo newborn22, if 可以删掉
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		jc.pool.Open(jc.env.Config().DB.AppConnector(), jc.env.Config().DB.DbaConnector(), jc.env.Config().DB.AppDebugConnector())

		jc.workingTables = map[string]bool{}
		jc.workingUUIDs = map[string]bool{}
		jc.jobChans = map[string]JobChanStruct{}
		jc.checkBeforeSchedule = make(chan struct{})

		go jc.jobHealthCheck(jc.checkBeforeSchedule)
		go jc.jobScheduler(jc.checkBeforeSchedule)
		initThrottleTicker()

	}
	return nil
}

func (jc *JobController) Close() {
	jc.pool.Close()
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

	// 检查字段

	// 将实例某些字段持久化写入表，能够crash后恢复
	// 将实例放入内存中某个地方
}

// todo newborn22 ， 能否改写得更有通用性? 这样改写是否好？
func (jc *JobController) HandleRequest(command, sql, jobUUID, tableSchema, expireString string, ratioLiteral *sqlparser.Literal, timeGapInMs, subtaskRows int64, postponeLaunch, autoRetry bool) (*sqltypes.Result, error) {
	// todo newborn22, if 可以删掉
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		switch command {
		case SubmitJob:
			return jc.SubmitJob(sql, tableSchema, timeGapInMs, subtaskRows, postponeLaunch, autoRetry)
		case ShowJobs:
			return jc.ShowJobs()
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
		}
	}
	// todo newborn22,对返回值判断为空？
	return nil, nil
}

// todo newboen22 函数的可见性，封装性上的改进？
// todo 传timegap和table_name
func (jc *JobController) SubmitJob(sql, tableSchema string, timeGapInMs, subtaskRows int64, postponeLaunch, autoRetry bool) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	jobUUID, err := schema.CreateUUIDWithDelimiter("-")
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	sql = rewirteSQL(sql)
	if timeGapInMs == 0 {
		timeGapInMs = int64(defaultTimeGap)
	}
	if subtaskRows == 0 {
		subtaskRows = int64(defaultSubtaskRows)
	}

	// todo，修改后的代码
	selectSQL, countSQLTemplate, tableName, err := jc.genSelectBatchKeySQL(sql, tableSchema)
	if err != nil {
		return nil, err
	}
	pkNames, err := jc.getTablePkName(ctx, tableSchema, tableName)
	if err != nil {
		return &sqltypes.Result{}, err
	}
	// todo，处理返回值
	jobBatchTable, err := jc.genBatchTable(jobUUID, selectSQL, countSQLTemplate, tableSchema, sql, pkNames, subtaskRows)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	jobStatus := queuedStatus
	if postponeLaunch {
		jobStatus = postponeLaunchStatus
	}

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobSubmit,
		sqltypes.StringBindVariable(jobUUID),
		sqltypes.StringBindVariable(sql),
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(jobBatchTable),
		sqltypes.Int64BindVariable(timeGapInMs),
		sqltypes.Int64BindVariable(subtaskRows),
		sqltypes.StringBindVariable(jobStatus))
	if err != nil {
		return nil, err
	}

	_, err = jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return &sqltypes.Result{}, err
	}
	// todo 增加 recursive-split，递归拆分batch的选项
	return jc.buildJobSubmitResult(jobUUID, jobBatchTable, timeGapInMs, subtaskRows, postponeLaunch, autoRetry), nil
}

func (jc *JobController) buildJobSubmitResult(jobUUID, jobBatchTable string, timeGap, subtaskRows int64, postponeLaunch, autoRetry bool) *sqltypes.Result {
	var rows []sqltypes.Row
	row := buildVarCharRow(jobUUID, jobBatchTable, strconv.FormatInt(timeGap, 10), strconv.FormatInt(subtaskRows, 10), strconv.FormatBool(autoRetry), strconv.FormatBool(postponeLaunch))
	rows = append(rows, row)
	submitRst := &sqltypes.Result{
		Fields:       buildVarCharFields("job_uuid", "job_batch_table", "time_gap_in_ms", "subtask_rows", "auto_retry", "postpone_launch"),
		Rows:         rows,
		RowsAffected: 1,
	}
	return submitRst
}

func (jc *JobController) ShowJobs() (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	ctx := context.Background()
	showJobsSQL := fmt.Sprintf("select * from %s", jc.tableName)
	return jc.execQuery(ctx, "mysql", showJobsSQL)
}

func (jc *JobController) PauseJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
	if err != nil {
		return emptyResult, err
	}
	if status != runningStatus {
		// todo，将info写回给vtgate，目前还不生效
		emptyResult.Info = " The job status is not running and can't be paused"
		return emptyResult, nil
	}

	// 往通道发送cmd进行暂停
	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	pauseChan := jc.jobChans[uuid].pauseAndResume
	pauseChan <- "pause"

	qr, err := jc.updateJobStatus(ctx, uuid, pausedStatus)
	if err != nil {
		return emptyResult, err
	}
	return qr, nil
}

func (jc *JobController) ResumeJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
	if err != nil {
		return emptyResult, err
	}
	if status != pausedStatus {
		emptyResult.Info = " The job status is not paused and don't need resume"
		return emptyResult, nil
	}
	// 往通道发送cmd以继续
	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	pauseChan := jc.jobChans[uuid].pauseAndResume
	pauseChan <- "resume"

	qr, err := jc.updateJobStatus(ctx, uuid, runningStatus)
	if err != nil {
		return emptyResult, err
	}

	return qr, nil
}

func (jc *JobController) LaunchJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
	if err != nil {
		return emptyResult, nil
	}
	if status != postponeLaunchStatus {
		emptyResult.Info = " The job status is not postpone-launch and don't need launch"
		return emptyResult, nil
	}
	return jc.updateJobStatus(ctx, uuid, queuedStatus)
}

func (jc *JobController) CancelJob(uuid string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()
	status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
	if err != nil {
		return emptyResult, nil
	}
	if status == canceledStatus || status == failedStatus || status == completedStatus {
		emptyResult.Info = fmt.Sprintf(" The job status is %s and can't canceld", status)
		return emptyResult, nil
	}

	qr, err := jc.updateJobStatus(ctx, uuid, canceledStatus)
	if err != nil {
		return emptyResult, nil
	}

	//	对于paused和running这两个状态的job，它们在内存中都有一个jobRunner协程正在运行，需要将协程关闭
	if status == runningStatus || status == pausedStatus {
		jc.jobChansMutex.Lock()
		cancelChan := jc.jobChans[uuid].cancel
		jc.jobChansMutex.Unlock()

		// 由于chan的容量为1，会阻塞在这里，直到jobRunner中接收了此信号
		cancelChan <- "cancel"
	}

	tableName, _ := jc.GetStrJobInfo(ctx, uuid, "related_table")

	jc.jobChansMutex.Lock()
	jc.workingUUIDsMutex.Lock()
	jc.workingTablesMutex.Lock()
	jc.deleteDMLJobRunningMeta(uuid, tableName)
	jc.jobChansMutex.Unlock()
	jc.workingUUIDsMutex.Unlock()
	jc.workingTablesMutex.Unlock()

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
			tick := time.NewTicker(throttleCheckDuration)
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
	delete(jc.workingTables, table)

	jc.workingUUIDsMutex.Lock()
	defer jc.workingUUIDsMutex.Unlock()
	delete(jc.workingUUIDs, uuid)

	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	close(jc.jobChans[uuid].pauseAndResume)
	close(jc.jobChans[uuid].cancel)
	delete(jc.jobChans, uuid)

	return jc.updateJobStatus(ctx, uuid, completedStatus)
}

// todo, 记录错误时的错误怎么处理
func (jc *JobController) FailJob(ctx context.Context, uuid, message, tableName string) {
	_ = jc.updateJobMessage(ctx, uuid, message)
	_, _ = jc.updateJobStatus(ctx, uuid, failedStatus)

	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()
	delete(jc.workingTables, tableName)

}

// todo newborn 做成接口
func jobTask() {
}

// 注意非primary要关掉
// todo 做成休眠和唤醒的
func (jc *JobController) jobScheduler(checkBeforeSchedule chan struct{}) {
	// 等待healthcare扫一遍后再进行

	<-checkBeforeSchedule
	fmt.Printf("start jobScheduler\n")

	ctx := context.Background()
	for {
		// todo,这里拿锁存在潜在bug，因为checkDmlJobRunnable中也拿了并去变成running状态，一个job可能被启动多次，要成睡眠和唤醒的方式
		// todo,优化这里的拿锁结构
		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()
		jc.workingUUIDsMutex.Lock()

		qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
		if qr == nil {
			continue
		}
		for _, row := range qr.Named().Rows {
			status := row["job_status"].ToString()
			schema := row["related_schema"].ToString()
			table := row["related_table"].ToString()
			uuid := row["job_uuid"].ToString()
			jobBatchTable := row["job_batch_table"].ToString()
			timegap, _ := row["timegap_in_ms"].ToInt64()
			//subtaskSQL := row["subtask_sql"].ToString()
			//dmlType := row["dml_type"].ToString()
			//countTotalRows, _ := row["count_total_rows"].ToInt64()
			//subtaskRows, _ := row["subtask_rows"].ToInt64()
			if jc.checkDmlJobRunnable(status, table) {
				// todo 这里之后改成休眠的方式后要删掉， 由于外面拿锁，必须在这里就加上，不然后面的循环可能：已经启动go runner的但是还未加入到working table,导致多个表的同时启动
				jc.initDMLJobRunningMeta(uuid, table)
				// go jc.dmlJobRunner(uuid, table, schema, subtaskSQL, dmlType, timegap, countTotalRows, 0, subtaskRows, true)
				go jc.dmlJobBatchRunner(uuid, table, schema, jobBatchTable, timegap)
			}
		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
		jc.workingUUIDsMutex.Unlock()

		time.Sleep(3 * time.Second)
	}
}

// 外部需要加锁
// todo，并发数的限制
func (jc *JobController) checkDmlJobRunnable(status, table string) bool {
	if status != queuedStatus {
		return false
	}
	if _, exit := jc.workingTables[table]; exit {
		return false
	}
	return true
}

func (jc *JobController) dmlJobRunner(uuid, table, relatedSchema, subtaskSQL, dmlType string, timeGap, countTotalRows, offset, subtaskRows int64, updateStatusRunning bool) {

	jc.jobChansMutex.Lock()
	jobChan := jc.jobChans[uuid]
	jc.jobChansMutex.Unlock()

	pauseAndResumeChan := jobChan.pauseAndResume
	cancelChan := jobChan.cancel

	// timeGap 单位ms，duration输入ns，应该乘上1000000
	timer := time.NewTicker(time.Duration(timeGap * 1e6))
	defer timer.Stop()

	var err error
	ctx := context.Background()

	if updateStatusRunning {
		_, err = jc.updateJobStatus(ctx, uuid, runningStatus)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
		}
	}

	// 在一个无限循环中等待定时器触发
	for {
		// 第一层select用于接收是否有用户的cancel命令，以随时结束协程
		select {
		case cmd := <-cancelChan:
			if cmd == "cancel" {
				_ = jc.updateJobMessage(ctx, uuid, fmt.Sprintf("Canceld by user at %s", time.Now().Format("2006-01-02 15:04:05")))
				return
			}
		default:
			select {
			case <-timer.C:
				// 定时器触发时执行的函数

				// 先请求throttle，若被throttle阻塞，则等待下一次timer事件
				if !jc.requestThrottle(uuid) {
					continue
				}

				// 获得sql，分update和delete两种情况
				var query string
				if dmlType == "update" {
					query, err = sqlparser.ParseAndBind(subtaskSQL, sqltypes.Int64BindVariable(offset))
					if err != nil {
						jc.FailJob(ctx, uuid, err.Error(), table)
					}
				}
				if dmlType == "delete" {
					query = subtaskSQL
				}

				affectedRows, err := jc.execSubtaskAndRecord(ctx, relatedSchema, query, uuid)

				if err != nil {
					jc.FailJob(ctx, uuid, err.Error(), table)
					return
				}

				// complete，分update和delete两种情况
				if (dmlType == "delete" && affectedRows == 0) || (dmlType == "update" && offset >= countTotalRows) {
					_, err = jc.CompleteJob(ctx, uuid, table)
					if err != nil {
						jc.FailJob(ctx, uuid, err.Error(), table)
					}
					return
				}

				if dmlType == "update" {
					//offset += int64(qr.RowsAffected)
					offset += subtaskRows
				}

			// 控制暂停
			case command := <-pauseAndResumeChan:
				switch command {
				case "pause":
					for {
						cmd := <-pauseAndResumeChan
						if cmd == "resume" { // actually, cmd will always be "resume", the code logic will guarantee that
							break
						}
					}
				}
			}
		}
	}

}

const (
	getDealingBatchIDSQL    = `select dealing_batch_id from mysql.big_dml_jobs_table where job_uuid = %a`
	updateDealingBatchIDSQL = `update mysql.big_dml_jobs_table set dealing_batch_id = %a where job_uuid = %a`
	getBatchSQLsByID        = `select batch_sql,batch_count_sql from %s where batch_id = %%a`
	getMaxBatchID           = `select max(batch_id) as max_batch_id from %s`
)

func (jc *JobController) getDealingBatchID(ctx context.Context, uuid string) (float64, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(getDealingBatchIDSQL,
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return 0, err
	}
	qr, err := jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, errors.New("the len of query result of batch ID is not one")
	}
	return qr.Named().Rows[0].ToFloat64("dealing_batch_id")
}

func (jc *JobController) updateDealingBatchID(ctx context.Context, uuid string, batchID float64) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(updateDealingBatchIDSQL,
		sqltypes.Float64BindVariable(batchID),
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

// todo to confirm，对于同一个job的batch表只有一个线程在访问，因此不用加锁
func (jc *JobController) getBatchSQLsByID(ctx context.Context, batchID float64, batchTableName, tableSchema string) (batchSQL, batchCountSQL string, err error) {
	getBatchSQLWithTableName := fmt.Sprintf(getBatchSQLsByID, batchTableName)
	query, err := sqlparser.ParseAndBind(getBatchSQLWithTableName,
		sqltypes.Float64BindVariable(batchID))
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
	batchCountSQL, _ = qr.Named().Rows[0].ToString("batch_count_sql")
	return batchSQL, batchCountSQL, nil
}

func (jc *JobController) getMaxBatchID(ctx context.Context, batchTableName, tableSchema string) (float64, error) {
	getMaxBatchIDWithTableName := fmt.Sprintf(getMaxBatchID, batchTableName)
	qr, err := jc.execQuery(ctx, tableSchema, getMaxBatchIDWithTableName)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, errors.New("the len of qr of getting batch sql by ID is not 1")
	}
	return qr.Named().Rows[0].ToFloat64("max_batch_id")
}

func (jc *JobController) execBatchAndRecord(ctx context.Context, tableSchema, batchSQL, batchCountSQL, uuid string, threshold int64, batchID float64) (nextBatchID float64, err error) {
	defer jc.env.LogError()

	var setting pools.Setting
	if tableSchema != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", tableSchema))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	defer conn.Recycle()
	if err != nil {
		return 0, err
	}

	// 1.开启事务
	_, err = conn.Exec(ctx, "start transaction", math.MaxInt32, true)
	if err != nil {
		return 0, err
	}

	// 2.查询batch sql预计影响的行数，如果超过阈值，则生成新的batch ID
	batchCountSQL += " FOR SHARE"
	qr, err := conn.Exec(ctx, batchCountSQL, math.MaxInt32, true)
	if err != nil {
		return 0, err
	}
	if len(qr.Named().Rows) != 1 {
		return 0, errors.New("the len of qr of count expected batch size is not 1")
	}
	expectedRow, _ := qr.Named().Rows[0].ToInt64("count_rows")
	if expectedRow > threshold {
		// todo，递归生成新的batch
		fmt.Printf("expectedRow > threshold")
	}

	// 3.执行batch sql
	_, err = conn.Exec(ctx, batchSQL, math.MaxInt32, true)
	if err != nil {
		return 0, err
	}

	// 4.更新正在处理的batch ID
	nextBatchID = batchID + 1 // todo，考虑生成新batch的情况，如何正确地获得下一个batch ID？
	err = jc.updateDealingBatchID(ctx, uuid, nextBatchID)
	if err != nil {
		return 0, err
	}

	// 5.提交事务
	_, err = conn.Exec(ctx, "commit", math.MaxInt32, true)
	if err != nil {
		return 0, err
	}
	return nextBatchID, nil
}

func (jc *JobController) dmlJobBatchRunner(uuid, table, relatedSchema, batchTable string, timeGap int64) {

	// timeGap 单位ms，duration输入ns，应该乘上1000000
	timer := time.NewTicker(time.Duration(timeGap * 1e6))
	defer timer.Stop()

	var err error
	ctx := context.Background()

	status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
	if err != nil {
		return
	}

	if status != runningStatus {
		_, err = jc.updateJobStatus(ctx, uuid, runningStatus)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
		err = jc.updateDealingBatchID(ctx, uuid, 1)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
	}
	currentBatchID, err := jc.getDealingBatchID(ctx, uuid)
	if err != nil {
		jc.FailJob(ctx, uuid, err.Error(), table)
		return
	}
	// todo，当动态生成batch时，如何更新max?
	maxBatchID, err := jc.getMaxBatchID(ctx, batchTable, relatedSchema)
	if err != nil {
		jc.FailJob(ctx, uuid, err.Error(), table)
		return
	}

	// 在一个无限循环中等待定时器触发
	for range timer.C {
		// 定时器触发时执行的函数
		status, err := jc.GetStrJobInfo(ctx, uuid, "job_status")
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
		// maybe paused / canceled
		if status != runningStatus {
			return
		}

		// 先请求throttle，若被throttle阻塞，则等待下一次timer事件
		if !jc.requestThrottle(uuid) {
			continue
		}

		batchSQL, batchCountSQL, err := jc.getBatchSQLsByID(ctx, currentBatchID, batchTable, relatedSchema)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}

		currentBatchID, err = jc.execBatchAndRecord(ctx, relatedSchema, batchSQL, batchCountSQL, uuid, defaultThreshold, currentBatchID)
		if err != nil {
			jc.FailJob(ctx, uuid, err.Error(), table)
			return
		}
		if currentBatchID > maxBatchID {
			_, err = jc.CompleteJob(ctx, uuid, table)
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error(), table)
				return
			}
		}
	}
}

// 注意在外面拿锁
func (jc *JobController) initDMLJobRunningMeta(uuid, table string) {
	// 容量为1：如果job crash前为pause，需要先往通道中写入"pause"，然后再启动jobRunner。如果不为1则会阻塞
	jobChan := JobChanStruct{pauseAndResume: make(chan string, 1), cancel: make(chan string)}
	//jc.jobChansMutex.Lock()
	jc.jobChans[uuid] = jobChan
	//jc.jobChansMutex.Unlock()

	//jc.workingTablesMutex.Lock()
	jc.workingTables[table] = true
	//jc.workingTablesMutex.Unlock()

	//jc.workingUUIDsMutex.Lock()
	jc.workingUUIDs[uuid] = true
	//jc.workingUUIDsMutex.Unlock()

}

// 注意在外面拿锁
func (jc *JobController) deleteDMLJobRunningMeta(uuid, table string) {
	// 容量为1：如果job crash前为pause，需要先往通道中写入"pause"，然后再启动jobRunner。如果不为1则会阻塞
	//jc.jobChansMutex.Lock()
	jobChan := jc.jobChans[uuid]
	close(jobChan.pauseAndResume)
	close(jobChan.cancel)
	delete(jc.jobChans, uuid)
	//jc.jobChansMutex.Unlock()

	//jc.workingTablesMutex.Lock()
	delete(jc.workingTables, table)
	//jc.workingTablesMutex.Unlock()

	//jc.workingUUIDsMutex.Lock()
	delete(jc.workingUUIDs, uuid)
	//jc.workingUUIDsMutex.Unlock()

}

// todo sql类型的判断换成别的方式
// todo 加行数字段
func (jc *JobController) genSubtaskDMLSQL(sql, tableSchema string, subtaskRows int64) (tableName, dmlType string, subTaskSQL, countTotalRowsSQL string, err error) {

	stmt, _, err := sqlparser.Parse2(sql)
	if err != nil {
		return "", "", "", "", err
	}
	switch s := stmt.(type) {
	case *sqlparser.Delete:
		if s.Limit != nil {
			return "", "", "", "", errors.New("the sql already has a LIMIT condition, can't be transferred to a DML job")
		}
		s.Limit = &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.FormatInt(subtaskRows, 10))}
		// todo，目前只支持单表
		if len(s.TableExprs) > 1 {
			return "", "", "", "", errors.New("the delete sql deals multi tables can't be transferred to a DML job")
		}
		tableName := sqlparser.String(s.TableExprs)
		whereStr := sqlparser.String(s.Where)
		if s.Where == nil {
			return "", "", "", "", errors.New("the sql without WHERE can't be transferred to a DML job")
		}
		countTotalRowsSQL = fmt.Sprintf("select count(*) from %s %s", tableName, whereStr)

		return tableName, "delete", sqlparser.String(s), countTotalRowsSQL, nil
	case *sqlparser.Update:
		// todo，最set中包含有pk的进行过滤
		// 如何获得一个表的pk/ pks?
		// update t set c1=v1,c2=v2... where P
		// update t temp1 join (select PK order by PK limit rows offset off t2) on t1.Pk=t2.Pk set temp1.c1=v1, temp1.c2=v2.. where P
		// 需要获得表名、set后的投影，where谓词的字符串，然后用字符串拼接的方式完成

		// select statement -> derivedTable -> joinTable
		// todo，目前只支持单表
		if len(s.TableExprs) > 1 {
			return "", "", "", "", errors.New("the update sql deals multi tables can't be transferred to a DML job")
		}
		tableName := sqlparser.String(s.TableExprs)
		ctx := context.Background()
		pkNames, err := jc.getTablePkName(ctx, tableSchema, tableName)
		if err != nil {
			return "", "", "", "", err
		}

		//if len(pkNames) > 1 {
		//	return "", "", "", "", errors.New("the update sql on table with multi Pks can't be transferred to a DML job")
		//}
		selectStr := "select "
		firstPK := true
		for _, pkName := range pkNames {
			if firstPK {
				selectStr += pkName
				firstPK = false
			} else {
				selectStr += " ,"
				selectStr += pkName
			}
		}
		selectStr += fmt.Sprintf(" from %s ", tableName)

		whereStr := sqlparser.String(s.Where)
		if s.Where == nil {
			return "", "", "", "", errors.New("the sql without WHERE can't be transferred to a DML job")
		}
		selectStr += whereStr

		firstPK = true
		selectStr += " order by "
		for _, pkName := range pkNames {
			if firstPK {
				selectStr += pkName
				firstPK = false
			} else {
				selectStr += " ,"
				selectStr += pkName
			}
		}

		selectStr += fmt.Sprintf(" limit %d offset %%a", subtaskRows)

		updateExprStr := sqlparser.String(s.Exprs)

		//colNames, err := jc.getTableColNames(ctx, tableSchema, tableName)
		//if err != nil {
		//	return "", "", err
		//}
		// whereStr = rewriteWhereStr(whereStr, "dml_job_temp_table222", colNames)

		subtaskSQL := ""
		if s.With != nil {
			subtaskSQL = sqlparser.String(s.With) + " "
		}

		joinOnConditionStr := ""
		firstPK = true
		for _, pkName := range pkNames {
			if firstPK {
				joinOnConditionStr += fmt.Sprintf("dml_job_temp_table111.%s = dml_job_temp_table222.%s ", pkName, pkName)
				firstPK = false
			} else {
				joinOnConditionStr += fmt.Sprintf("AND dml_job_temp_table111.%s = dml_job_temp_table222.%s", pkName, pkName)
			}
		}

		subtaskSQL += fmt.Sprintf("UPDATE %s dml_job_temp_table111 JOIN (%s) dml_job_temp_table222 ON %s SET %s",
			tableName, selectStr, joinOnConditionStr, updateExprStr)

		//selectStmt, _, err := sqlparser.Parse2(selectStr)
		//if err != nil {
		//	return "", err
		//}
		//joinLeftExpr := sqlparser.AliasedTableExpr{Expr:  sqlparser.TableName{Name: sqlparser.IdentifierCS{}}}
		//sqlparser.JoinTableExpr{Join: sqlparser.NormalJoinType,LeftExpr: }

		// 将 "=" 替换成 "!="
		rewriteExprStr := strings.Replace(updateExprStr, "=", "!=", -1)
		// 将 "," 替换成 "AND"
		rewriteExprStr = strings.Replace(rewriteExprStr, ",", "AND", -1)

		countTotalRowsSQL = fmt.Sprintf("select count(*) from %s %s", tableName, whereStr) + " AND " + rewriteExprStr

		return tableName, "update", subtaskSQL, countTotalRowsSQL, nil

	}
	return "", "", "", "", errors.New("the sql type can't be transferred to a DML job")
}

// execQuery execute sql by using connect poll,so if targetString is not empty, it will add prefix `use database` first then execute sql.
func (jc *JobController) execQuery(ctx context.Context, targetString, query string) (result *sqltypes.Result, err error) {
	defer jc.env.LogError()
	var setting pools.Setting
	if targetString != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", targetString))
	}
	conn, err := jc.pool.Get(ctx, &setting)
	if err != nil {
		return result, err
	}
	defer conn.Recycle()
	return conn.Exec(ctx, query, math.MaxInt32, true)
}

func (jc *JobController) execSubtaskAndRecord(ctx context.Context, tableSchema, subtaskSQL, uuid string) (affectedRows int64, err error) {
	defer jc.env.LogError()

	var setting pools.Setting
	if tableSchema != "" {
		setting.SetWithoutDBName(false)
		setting.SetQuery(fmt.Sprintf("use %s", tableSchema))
	}
	// todo ，是不是有事务专门的连接池？需要看一下代码
	conn, err := jc.pool.Get(ctx, &setting)
	defer conn.Recycle()
	if err != nil {
		return 0, err
	}

	_, err = conn.Exec(ctx, "start transaction", math.MaxInt32, true)
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
	_, err = conn.Exec(ctx, recordRstSQL, math.MaxInt32, true)
	if err != nil {
		return 0, err
	}
	_, err = conn.Exec(ctx, "commit", math.MaxInt32, true)
	if err != nil {
		return 0, err
	}

	return affectedRows, nil
}

func rewirteSQL(input string) string {
	// 定义正则表达式匹配注释
	re := regexp.MustCompile(`/\*.*?\*/`)
	// 用空字符串替换匹配到的注释
	result := re.ReplaceAllString(input, "")
	return result
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

func (jc *JobController) updateJobStatus(ctx context.Context, uuid, status string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
		sqltypes.StringBindVariable(status),
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

func (jc *JobController) getTablePkName(ctx context.Context, tableSchema, tableName string) ([]string, error) {
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
	return pkNames, nil
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

func rewriteWhereStr(whereStr, subQueryTableName string, colNames []string) string {

	// 使用正则表达式匹配单词
	re := regexp.MustCompile(`\b\w+(\.\w+)?\b`)
	modifiedStr := re.ReplaceAllStringFunc(whereStr, func(match string) string {
		// 检查匹配的单词是否在 colNames 中或者是否以 'mytable.' 开头
		parts := strings.Split(match, ".")
		if len(parts) > 1 {
			if contains(colNames, parts[1]) {
				return subQueryTableName + "." + parts[1]
			}
		} else if contains(colNames, match) {
			return subQueryTableName + "." + match
		}
		return match
	})
	return modifiedStr
}

func contains(arr []string, str string) bool {
	for _, v := range arr {
		if v == str {
			return true
		}
	}
	return false
}

func (jc *JobController) jobHealthCheck(checkBeforeSchedule chan struct{}) {
	ctx := context.Background()

	// 用于crash后，重启时，先扫一遍running和paused的
	qr, _ := jc.execQuery(ctx, "", sqlDMLJobGetAllJobs)
	if qr != nil {

		jc.workingTablesMutex.Lock()
		jc.tableMutex.Lock()
		jc.workingUUIDsMutex.Lock()

		for _, row := range qr.Named().Rows {
			status := row["job_status"].ToString()
			tableSchema := row["related_schema"].ToString()
			table := row["related_table"].ToString()
			uuid := row["job_uuid"].ToString()
			timegap, _ := row["timegap_in_ms"].ToInt64()
			subtaskSQL := row["subtask_sql"].ToString()
			dmlType := row["dml_type"].ToString()
			countTotalRows, _ := row["count_total_rows"].ToInt64()
			AffectedRows, _ := row["affected_rows"].ToInt64()
			subtaskRows, _ := row["subtask_rows"].ToInt64()

			if status == runningStatus {
				jc.initDMLJobRunningMeta(uuid, table)
				go jc.dmlJobRunner(uuid, table, tableSchema, subtaskSQL, dmlType, timegap, countTotalRows, AffectedRows, subtaskRows, false)
			}
			if status == pausedStatus {
				jc.initDMLJobRunningMeta(uuid, table)
				// 触发暂停
				jc.jobChansMutex.Lock()
				pauseChan := jc.jobChans[uuid].pauseAndResume
				jc.jobChansMutex.Unlock()
				pauseChan <- "pause"
				go jc.dmlJobRunner(uuid, table, tableSchema, subtaskSQL, dmlType, timegap, countTotalRows, AffectedRows, subtaskRows, false)
			}
		}

		jc.workingTablesMutex.Unlock()
		jc.tableMutex.Unlock()
		jc.workingUUIDsMutex.Unlock()
	}

	fmt.Printf("check of running and paused done \n")
	checkBeforeSchedule <- struct{}{}

	for {

		// todo, 增加对长时间未增加 rows的处理
		// todo，对于cancel和failed 垃圾条目的删除

		time.Sleep(healthCheckTimeGap)
	}
}

// 目前只支持
// 1.PK作为拆分列,支持多列PK todo 支持UK或其他列
// 2.目前只支持单表，且没有join
func (jc *JobController) genSelectBatchKeySQL(sql, tableSchema string) (selectSQL, countSQLTemplate, tableName string, err error) {
	// SELECT `id` FROM `test`.`t` WHERE (`v` < 6) ORDER BY IF(ISNULL(`id`),0,1),`id`， 由于是PK，因此不需要判断ISNULL
	stmt, _, err := sqlparser.Parse2(sql)
	if err != nil {
		return "", "", "", err
	}
	wherePart := ""
	switch s := stmt.(type) {
	case *sqlparser.Delete:
		if len(s.TableExprs) != 1 {
			return "", "", "", errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		// 目前暂不支持join和多表 todo
		if !ok {
			return "", "", "", errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		wherePart = sqlparser.String(s.Where)
	case *sqlparser.Update:
		if len(s.TableExprs) != 1 {
			return "", "", "", errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		// 目前暂不支持join和多表 todo
		if !ok {
			return "", "", "", errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		wherePart = sqlparser.String(s.Where)
	}

	// 选择出PK
	ctx := context.Background()
	pkNames, err := jc.getTablePkName(ctx, tableSchema, tableName)
	if err != nil {
		return "", "", "", err
	}
	PKPart := ""
	firstPK := true
	for _, pkName := range pkNames {
		if !firstPK {
			PKPart += ","
		}
		PKPart += pkName
		firstPK = false
	}

	selectSQL = fmt.Sprintf("select %s from %s.%s %s order by %s",
		PKPart, tableSchema, tableName, wherePart, PKPart)

	// todo，支持多PK多类型
	countSQLTemplate = fmt.Sprintf("select count(*) as count_rows from %s.%s %s AND %s between %%d AND %%d order by %s",
		tableSchema, tableName, wherePart, PKPart, PKPart)

	return selectSQL, countSQLTemplate, tableName, nil
}

const (
	insertBatchSQL = ` insert into %s (
		batch_id,
		batch_sql,
	 	batch_count_sql,
		batch_size
	) values (%%a,%%a,%%a,%%a)`
)

// 创建表，todo 表gc
// todo，discussion，建表的过程中需要放在一个事务内，防止崩了,由于一个事务容纳的数据有限，oom，因此需要多个事务?
func (jc *JobController) genBatchTable(jobUUID, selectSQL, countSQLTemplate, tableSchema, sql string, pkNames []string, batchSize int64) (string, error) {
	ctx := context.Background()

	qr, err := jc.execQuery(ctx, "", selectSQL)
	if err != nil {
		return "", err
	}
	if len(qr.Named().Rows) == 0 {
		return "", nil
	}

	// 建表
	batchTableName := "job_batch_table_" + strings.Replace(jobUUID, "-", "_", -1)
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		id                              bigint unsigned  NOT NULL AUTO_INCREMENT,
		batch_id                              DOUBLE NOT NULL, 
		batch_sql                       varchar(1024)     NOT NULL,
    	batch_count_sql                       varchar(1024)     NOT NULL,
    	batch_size 						bigint unsigned  NOT NULL,
		PRIMARY KEY (id)
	) ENGINE = InnoDB`, batchTableName)

	_, err = jc.execQuery(ctx, tableSchema, createTableSQL)
	if err != nil {
		return "", err
	}

	// todo，处理多pk的情况
	// todo 处理不同类型的PK
	// todo 创建一个新的value结构，表示PK的名字和类型
	currentBatchSize := int64(0)
	currentBatchStart := int64(0)
	currentBatchEnd := int64(0)
	currentBatchID := float64(1)

	insertBatchSQLWithTableName := fmt.Sprintf(insertBatchSQL, batchTableName)

	var pkName string
	// todo 对结果集为0的情况进行特判
	for _, row := range qr.Named().Rows {
		for _, pkName = range pkNames {
			// todo，对于每一列PK的值类型进行判断
			keyVal, _ := row[pkName].ToInt64()

			if currentBatchSize == 0 {
				currentBatchStart = keyVal
			}
			currentBatchEnd = keyVal
			currentBatchSize++ // 改成多pk后要移出去 todo
			if currentBatchSize == batchSize {
				// between是一个闭区间，batch job的sql也是闭区间
				// todo 处理整数之外的类型
				batchSQL := sql + fmt.Sprintf(" AND %s between %d AND %d", pkName, currentBatchStart, currentBatchEnd)
				countSQL := fmt.Sprintf(countSQLTemplate, currentBatchStart, currentBatchEnd)

				currentBatchSize = 0
				// insert into table
				insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQLWithTableName,
					sqltypes.Float64BindVariable(currentBatchID),
					sqltypes.StringBindVariable(batchSQL),
					sqltypes.StringBindVariable(countSQL),
					sqltypes.Int64BindVariable(int64(batchSize)))
				if err != nil {
					return "", err
				}
				_, err = jc.execQuery(ctx, tableSchema, insertBatchSQLQuery)
				if err != nil {
					return "", err
				}
				currentBatchID++ // 改成多pk后要移出循环 todo
			}
		}
	}
	//最后一个batch
	if currentBatchSize != 0 {
		batchSQL := sql + fmt.Sprintf(" AND %s between %d AND %d", pkName, currentBatchStart, currentBatchEnd)
		countSQL := fmt.Sprintf(countSQLTemplate, currentBatchStart, currentBatchEnd)
		insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQLWithTableName,
			sqltypes.Float64BindVariable(currentBatchID),
			sqltypes.StringBindVariable(batchSQL),
			sqltypes.StringBindVariable(countSQL),
			sqltypes.Int64BindVariable(int64(currentBatchSize)))
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
