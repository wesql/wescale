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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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
	pausedStatus         = "paused"
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
                                      subtask_sql,
                                      dml_type,
                                      job_status) values(%a,%a,%a,%a,%a,%a,%a,%a,%a)`

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
)

type JobController struct {
	tableName      string
	tableMutex     sync.Mutex // todo newborn22,检查是否都上锁了
	tabletTypeFunc func() topodatapb.TabletType
	env            tabletenv.Env
	pool           *connpool.Pool

	workingTables      map[string]bool // 用于调度时检测当前任务是否和正在工作的表冲突，paused、running状态的job的表都在里面
	workingTablesMutex sync.Mutex

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

		jc.workingTables = map[string]bool{}
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
		case PauseJob:
			return jc.PauseJob(jobUUID)
		case ResumeJob:
			return jc.ResumeJob(jobUUID)
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
	relatedSchema := "mydb"                  // todo，传入
	timeGap := int64(defaultTimeGap)         // todo 传入
	subtaskRows := int64(defaultSubtaskRows) // todo 传入
	// todo，改写，根据stmt
	// todo 对于jobrunner，要区分update和delete，update每次都要更新执行的limit值。
	table, dmlType, subtaskSQL, err := jc.genSubtaskDMLSQL(sql, relatedSchema, subtaskRows)
	if err != nil {
		return &sqltypes.Result{}, err
	}

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobSubmit,
		sqltypes.StringBindVariable(jobUUID),
		sqltypes.StringBindVariable(sql),
		sqltypes.StringBindVariable(relatedSchema),
		sqltypes.StringBindVariable(table),
		sqltypes.Int64BindVariable(timeGap),
		sqltypes.Int64BindVariable(subtaskRows),
		sqltypes.StringBindVariable(subtaskSQL),
		sqltypes.StringBindVariable(dmlType),
		sqltypes.StringBindVariable(queuedStatus))
	if err != nil {
		return nil, err
	}

	jobChan := JobChanStruct{pauseAndResume: make(chan string), throttleAndUnthrottle: make(chan string)}
	jc.jobChans[jobUUID] = jobChan

	_, err = jc.execQuery(ctx, "", submitQuery)
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.buildJobSubmitResult(jobUUID, subtaskSQL, timeGap, subtaskRows), nil
}

func (jc *JobController) buildJobSubmitResult(jobUUID, subtaskSQL string, timeGap, subtaskRows int64) *sqltypes.Result {
	var rows []sqltypes.Row
	row := buildVarCharRow(jobUUID, strconv.FormatInt(timeGap, 10), "support later", strconv.FormatInt(subtaskRows, 10), subtaskSQL, "support later", "support later")
	rows = append(rows, row)
	submitRst := &sqltypes.Result{
		Fields:       buildVarCharFields("job_uuid", "time_gap_in_ms", "total_rows", "subtask_rows", "subtask_sql", "auto_retry", "postpone_launch"),
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

	qr, err := jc.updateJobStatus(ctx, uuid, pausedStatus)
	if err != nil {
		return emptyResult, err
	}

	// 往通道发送cmd进行暂停
	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	pauseChan := jc.jobChans[uuid].pauseAndResume
	pauseChan <- "pause"
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

	qr, err := jc.updateJobStatus(ctx, uuid, runningStatus)
	if err != nil {
		return emptyResult, err
	}

	// 往通道发送cmd以继续
	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	pauseChan := jc.jobChans[uuid].pauseAndResume
	pauseChan <- "resume"
	return qr, nil
}

func (jc *JobController) CompleteJob(ctx context.Context, uuid, table string) (*sqltypes.Result, error) {
	jc.workingTablesMutex.Lock()
	defer jc.workingTablesMutex.Unlock()
	delete(jc.workingTables, table)

	jc.jobChansMutex.Lock()
	defer jc.jobChansMutex.Unlock()
	close(jc.jobChans[uuid].pauseAndResume)
	close(jc.jobChans[uuid].throttleAndUnthrottle)
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
func (jc *JobController) jonScheduler() {
	ctx := context.Background()
	for {
		// todo,这里拿锁存在潜在bug，因为checkDmlJobRunnable中也拿了并去变成running状态，一个job可能被启动多次，要成睡眠和唤醒的方式
		jc.workingTablesMutex.Lock()
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
			timegap, _ := row["timegap_in_ms"].ToInt64()
			subtaskSQL := row["subtask_sql"].ToString()
			dmlType := row["dml_type"].ToString()
			if jc.checkDmlJobRunnable(status, table) {
				jc.workingTables[table] = true // todo 这里之后改成休眠的方式后要删掉
				go jc.dmlJobRunner(uuid, table, schema, subtaskSQL, dmlType, timegap)
			}
		}

		jc.workingTablesMutex.Unlock()
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
	if _, exit := jc.workingTables[table]; exit {
		return false
	}
	return true
}

func (jc *JobController) dmlJobRunner(uuid, table, relatedSchema, subtaskSQL, dmlType string, timeGap int64) {

	jc.jobChansMutex.Lock()
	jobChan := jc.jobChans[uuid]
	jc.jobChansMutex.Unlock()

	pauseAndResumeChan := jobChan.pauseAndResume

	// timeGap 单位ms，duration输入ns，应该乘上1000000
	timer := time.NewTicker(time.Duration(timeGap * 1e6))
	defer timer.Stop()

	ctx := context.Background()

	jc.workingTablesMutex.Lock()
	jc.workingTables[table] = true
	jc.workingTablesMutex.Unlock()
	_, err := jc.updateJobStatus(ctx, uuid, runningStatus)
	if err != nil {
		jc.FailJob(ctx, uuid, err.Error(), table)
	}

	offset := int64(0)

	// 在一个无限循环中等待定时器触发
	for {
		select {
		case <-timer.C:
			// 定时器触发时执行的函数
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
			qr, err := jc.execQuery(ctx, relatedSchema, query)
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error(), table)
				return
			}
			if qr.RowsAffected == 0 {
				_, err = jc.CompleteJob(ctx, uuid, table)
				if err != nil {
					jc.FailJob(ctx, uuid, err.Error(), table)
				}
				return
			}
			err = jc.updateJobAffectedRows(ctx, uuid, int64(qr.RowsAffected))
			if err != nil {
				jc.FailJob(ctx, uuid, err.Error(), table)
				return
			}
			if dmlType == "update" {
				offset += int64(qr.RowsAffected)
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
func (jc *JobController) genSubtaskDMLSQL(sql, tableSchema string, subtaskRows int64) (tableName, dmlType string, subTaskSQL string, err error) {

	stmt, _, err := sqlparser.Parse2(sql)
	if err != nil {
		return "", "", "", err
	}
	switch s := stmt.(type) {
	case *sqlparser.Delete:
		if s.Limit != nil {
			return "", "", "", errors.New("the sql already has a LIMIT condition, can't be transferred to a DML job")
		}
		s.Limit = &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.FormatInt(subtaskRows, 10))}
		// todo，目前只支持单表
		if len(s.TableExprs) > 1 {
			return "", "", "", errors.New("the delete sql deals multi tables can't be transferred to a DML job")
		}
		tableName := sqlparser.String(s.TableExprs)
		return tableName, "delete", sqlparser.String(s), nil
	case *sqlparser.Update:
		// todo，最set中包含有pk的进行过滤
		// 如何获得一个表的pk/ pks?
		// update t set c1=v1,c2=v2... where P
		// update t temp1 join (select PK order by PK limit rows offset off t2) on t1.Pk=t2.Pk set temp1.c1=v1, temp1.c2=v2.. where P
		// 需要获得表名、set后的投影，where谓词的字符串，然后用字符串拼接的方式完成

		// select statement -> derivedTable -> joinTable
		// todo，目前只支持单表
		if len(s.TableExprs) > 1 {
			return "", "", "", errors.New("the update sql deals multi tables can't be transferred to a DML job")
		}
		tableName := sqlparser.String(s.TableExprs)
		ctx := context.Background()
		pkNames, err := jc.getTablePkName(ctx, tableSchema, tableName)
		if err != nil {
			return "", "", "", err
		}
		// todo ，目前只支持单个PK
		if len(pkNames) > 1 {
			return "", "", "", errors.New("the update sql on table with multi Pks can't be transferred to a DML job")
		}
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

		pkName := pkNames[0]

		subtaskSQL := ""
		if s.With != nil {
			subtaskSQL = sqlparser.String(s.With) + " "
		}
		subtaskSQL += fmt.Sprintf("UPDATE %s dml_job_temp_table111 JOIN (%s) dml_job_temp_table222 ON dml_job_temp_table111.%s = dml_job_temp_table222.%s SET %s",
			tableName, selectStr, pkName, pkName, updateExprStr)

		//selectStmt, _, err := sqlparser.Parse2(selectStr)
		//if err != nil {
		//	return "", err
		//}
		//joinLeftExpr := sqlparser.AliasedTableExpr{Expr:  sqlparser.TableName{Name: sqlparser.IdentifierCS{}}}
		//sqlparser.JoinTableExpr{Join: sqlparser.NormalJoinType,LeftExpr: }
		return tableName, "update", subtaskSQL, nil

	}
	return "", "", "", errors.New("the sql type can't be transferred to a DML job")
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
