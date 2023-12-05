/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// todo newborn22, 数一下连接数是不是3够用
const (
	databasePoolSize = 3
)

const (
	SubmitJob = "submit_job"
	ShowJobs  = "show_jobs"
)

// todo ，支持用户输入
const (
	defaultTimeGap     = 1000 // 1000ms
	defaultSubtaskRows = 100
)

const (
	postponeLaunchStatus = "postpone-launch"
	queuedStatus         = "queued"
	blockedStatus        = "blocked"
	runningStatus        = "running"
	interruptedStatus    = "interrupted"
	canceledStatus       = "canceled"
	failedStatus         = "failed"
	completedStatus      = "completed"
)

const (
	sqlDMLJobSubmit = `insert into mysql.big_dml_jobs_table (
                                      job_uuid,
                                      dml_sql,
                                      related_schema,
                                      related_table,
                                      timegap_in_ms,
                                      subtask_rows,
                                      job_status) values(%a,%a,%a,%a,%a,%a,%a)`

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
)

type JobController struct {
	tableName      string
	tableMutex     sync.Mutex // todo newborn22,检查是否都上锁了
	tabletTypeFunc func() topodatapb.TabletType
	env            tabletenv.Env
	pool           *connpool.Pool

	runningTables      map[string]bool // 用于调度时检测当前任务是否和正在运行的表冲突
	runningTablesMutex sync.Mutex

	jobChans      map[string]JobChanStruct
	jobChansMutex sync.Mutex
}

type JobChanStruct struct {
	pauseAndResume        chan string
	throttleAndUnthrottle chan string
}

// todo newborn22, 初始化函数
// 要加锁？
func (jc *JobController) Open() error {
	// todo newborn22 ，改成英文注释
	// 只在primary上运行，记得在rpc那里也做处理
	// todo newborn22, if 可以删掉
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		jc.pool.Open(jc.env.Config().DB.AppConnector(), jc.env.Config().DB.DbaConnector(), jc.env.Config().DB.AppDebugConnector())

		jc.runningTables = map[string]bool{}
		jc.jobChans = map[string]JobChanStruct{}

		go jc.jonScheduler()
	}
	return nil
}

func (jc *JobController) Close() {
	jc.pool.Close()
}

func NewJobController(tableName string, tabletTypeFunc func() topodatapb.TabletType, env tabletenv.Env) *JobController {
	return &JobController{
		tableName:      tableName,
		tabletTypeFunc: tabletTypeFunc,
		env:            env,
		pool: connpool.NewPool(env, "DMLJobControllerPool", tabletenv.ConnPoolConfig{
			Size:               databasePoolSize,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		})}

	// 检查字段

	// 将实例某些字段持久化写入表，能够crash后恢复
	// 将实例放入内存中某个地方
}

// todo newborn22 ， 能否改写得更有通用性? 这样改写是否好？
func (jc *JobController) HandleRequest(command, sql, jobUUID string) (*sqltypes.Result, error) {
	// todo newborn22, if 可以删掉
	if jc.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
		switch command {
		case SubmitJob:
			return jc.SubmitJob(sql)
		case ShowJobs:
			return jc.ShowJobs()
		}
	}
	// todo newborn22,对返回值判断为空？
	return nil, nil
}

// todo newboen22 函数的可见性，封装性上的改进？
// todo 传timegap和table_name
func (jc *JobController) SubmitJob(sql string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()

	jobUUID, err := schema.CreateUUIDWithDelimiter("-")
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	sql = rewirteSQL(sql)
	relatedSchema := "mydb" // todo，传入
	table := "test_table2"  // todo ,前端传入
	timeGap := defaultTimeGap
	subtaskRows := defaultSubtaskRows

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobSubmit,
		sqltypes.StringBindVariable(jobUUID),
		sqltypes.StringBindVariable(sql),
		sqltypes.StringBindVariable(relatedSchema),
		sqltypes.StringBindVariable(table),
		sqltypes.Int64BindVariable(int64(timeGap)),
		sqltypes.Int64BindVariable(int64(subtaskRows)),
		sqltypes.StringBindVariable(queuedStatus))
	if err != nil {
		return nil, err
	}

	jobChan := JobChanStruct{pauseAndResume: make(chan string), throttleAndUnthrottle: make(chan string)}
	jc.jobChans[jobUUID] = jobChan

	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) ShowJobs() (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()
	ctx := context.Background()
	showJobsSQL := fmt.Sprintf("select * from %s", jc.tableName)
	return jc.execQuery(ctx, "mysql", showJobsSQL)
}

func (jc *JobController) ControlJob() error {
	return nil
}

func (jc *JobController) RunJob() error {
	return nil
}

func (jc *JobController) QueryJob() error {
	return nil
}

func (jc *JobController) CompleteJob(ctx context.Context, uuid, table string) error {
	jc.runningTablesMutex.Lock()
	defer jc.runningTablesMutex.Unlock()
	delete(jc.runningTables, table)

	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	close(jc.jobChans[uuid].pauseAndResume)
	close(jc.jobChans[uuid].throttleAndUnthrottle)
	delete(jc.jobChans, uuid)

	return jc.updateJobStatus(ctx, uuid, completedStatus)
}

// todo, 记录错误时的错误怎么处理
func (jc *JobController) FailJob(ctx context.Context, uuid, message string) {
	_ = jc.updateJobMessage(ctx, uuid, message)
	_ = jc.updateJobStatus(ctx, uuid, failedStatus)
}

// todo newborn 做成接口
func jobTask() {
}

// 注意非primary要关掉
// todo 做成休眠和唤醒的
func (jc *JobController) jonScheduler() {
	ctx := context.Background()
	for {
		jc.runningTablesMutex.Lock()
		jc.tableMutex.Lock()

		qr, _ := jc.execQuery(ctx, "", "select * from mysql.big_dml_jobs_table;")
		if qr == nil {
			continue
		}
		for _, row := range qr.Named().Rows {
			status := row["job_status"].ToString()
			schema := row["related_schema"].ToString()
			table := row["related_table"].ToString()
			uuid := row["job_uuid"].ToString()
			sql := row["dml_sql"].ToString()
			timegap, _ := row["timegap_in_ms"].ToInt64()
			subtaskRows, _ := row["subtask_rows"].ToInt64()
			if jc.checkDmlJobRunnable(status, table) {
				go jc.dmlJobRunner(uuid, table, sql, schema, subtaskRows, timegap)
			}
		}

		jc.runningTablesMutex.Unlock()
		jc.tableMutex.Unlock()

		time.Sleep(3 * time.Second)
	}
}

// 外部需要加锁
// todo，并发数的限制
func (jc *JobController) checkDmlJobRunnable(status, table string) bool {
	if status != queuedStatus {
		return false
	}
	if _, exit := jc.runningTables[table]; exit {
		return false
	}
	return true
}

func (jc *JobController) dmlJobRunner(uuid, table, sql, relatedSchema string, subtaskRows, timeGap int64) {
	subtaskSQL := genSubtaskDMLSQL(sql, subtaskRows)
	jc.jobChansMutex.Lock()
	jobChan := jc.jobChans[uuid]
	jc.jobChansMutex.Unlock()

	pauseAndResumeChan := jobChan.pauseAndResume

	// timeGap 单位ms，duration输入ns，应该乘上1000000
	timer := time.NewTicker(time.Duration(timeGap * 1e6))
	defer timer.Stop()

	ctx := context.Background()

	jc.runningTablesMutex.Lock()
	jc.runningTables[table] = true
	jc.runningTablesMutex.Unlock()
	_ = jc.updateJobStatus(ctx, uuid, runningStatus)

	// 在一个无限循环中等待定时器触发
	for {
		select {
		case <-timer.C:
			// 定时器触发时执行的函数
			qr, err := jc.execQuery(ctx, relatedSchema, subtaskSQL)
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error())
				return
			}
			if qr.RowsAffected == 0 {
				err = jc.CompleteJob(ctx, uuid, table)
				if err != nil {
					jc.FailJob(ctx, uuid, err.Error())
				}
				return
			}
			err = jc.updateJobAffectedRows(ctx, uuid, int64(qr.RowsAffected))
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error())
				return
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

// todo sql类型的判断换成别的方式
// todo 加行数字段
func genSubtaskDMLSQL(sql string, subtaskRows int64) string {
	var subtaskSQL string
	sqlType := strings.ToLower(strings.Fields(sql)[0])
	switch sqlType {
	case "delete":
		subtaskSQL = sql + fmt.Sprintf(" LIMIT %d", subtaskRows)
	}
	return subtaskSQL
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

func (jc *JobController) updateJobStatus(ctx context.Context, uuid, status string) error {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateStatus,
		sqltypes.StringBindVariable(status),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, "", submitQuery)
	return err
}
