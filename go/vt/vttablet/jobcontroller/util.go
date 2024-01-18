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
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

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

func (jc *JobController) buildJobSubmitResult(jobUUID, jobBatchTable string, timeGap, subtaskRows int64, postponeLaunch bool, failPolicy string) *sqltypes.Result {
	var rows []sqltypes.Row
	row := buildVarCharRow(jobUUID, jobBatchTable, strconv.FormatInt(timeGap, 10), strconv.FormatInt(subtaskRows, 10), failPolicy, strconv.FormatBool(postponeLaunch))
	rows = append(rows, row)
	submitRst := &sqltypes.Result{
		Fields:       buildVarCharFields("job_uuid", "batch_info_table_name", "batch_interval_in_ms", "batch_size", "fail_policy", "postpone_launch"),
		Rows:         rows,
		RowsAffected: 1,
	}
	return submitRst
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
	defer conn.Recycle()
	if err != nil {
		return result, err
	}
	qr, err := conn.Exec(ctx, query, math.MaxInt32, true)
	return qr, err

}

func parseDML(sql string) (tableName string, whereExpr sqlparser.Expr, stmt sqlparser.Statement, err error) {
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		return "", nil, nil, err
	}
	// 根据stmt，分析DML SQL的各个部分，包括涉及的表，where条件
	switch s := stmt.(type) {
	case *sqlparser.Delete:
		if len(s.TableExprs) != 1 {
			return "", nil, nil, errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		// todo feat 目前暂不支持join和多表
		if !ok {
			return "", nil, nil, errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		// the sql should have where clause
		if s.Where == nil {
			return "", nil, nil, errors.New("the SQL should have where clause")
		}
		whereExpr = s.Where.Expr
		// the sql should not have limit clause and order by clause
		limitPart := sqlparser.String(s.Limit)
		if limitPart != "" {
			return "", nil, nil, errors.New("the SQL should not have limit clause")
		}
		orderByPart := sqlparser.String(s.OrderBy)
		if orderByPart != "" {
			return "", nil, nil, errors.New("the SQL should not have order by clause")
		}

	case *sqlparser.Update:
		if len(s.TableExprs) != 1 {
			return "", nil, nil, errors.New("the number of table is more than one")
		}
		tableExpr, ok := s.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return "", nil, nil, errors.New("don't support join table now")
		}
		tableName = sqlparser.String(tableExpr)
		// the sql should have where clause
		if s.Where == nil {
			return "", nil, nil, errors.New("the SQL should have where clause")
		}
		whereExpr = s.Where.Expr
		// the sql should not have limit clause and order by clause
		limitPart := sqlparser.String(s.Limit)
		if limitPart != "" {
			return "", nil, nil, errors.New("the SQL should not have limit clause")
		}
		orderByPart := sqlparser.String(s.OrderBy)
		if orderByPart != "" {
			return "", nil, nil, errors.New("the SQL should not have order by clause")
		}

	default:
		// todo feat support select...into, replace...into
		return "", nil, nil, errors.New("the type of sql is not supported")
	}

	if err != nil {
		return "", nil, nil, err
	}

	return tableName, whereExpr, stmt, err
}

func sprintfSelectPksSQL(tableName, whereStr string, pkInfos []PKInfo) string {
	pkCols := ""
	firstPK := true
	for _, pkInfo := range pkInfos {
		if !firstPK {
			pkCols += ","
		}
		pkCols += pkInfo.pkName
		firstPK = false
	}
	selectPksSQL := fmt.Sprintf("select %s from %s where %s order by %s",
		pkCols, tableName, whereStr, pkCols)
	return selectPksSQL
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

func (jc *JobController) updateJobPeriodTime(ctx context.Context, uuid, timePeriodStart, timePeriodEnd, timeZone string) (*sqltypes.Result, error) {
	jc.tableMutex.Lock()
	defer jc.tableMutex.Unlock()

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobUpdateTimePeriod,
		sqltypes.StringBindVariable(timePeriodStart),
		sqltypes.StringBindVariable(timePeriodEnd),
		sqltypes.StringBindVariable(timeZone),
		sqltypes.StringBindVariable(uuid))
	if err != nil {
		return &sqltypes.Result{}, err
	}
	return jc.execQuery(ctx, "", submitQuery)
}

func (jc *JobController) getIntJobInfo(ctx context.Context, uuid, fieldName string) (int64, error) {
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

func (jc *JobController) getStrJobInfo(ctx context.Context, uuid, fieldName string) (string, error) {
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

// todo feat batch的并行执行，注意对batch表的操作
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

func (jc *JobController) getTablePkInfo(ctx context.Context, tableSchema, tableName string) ([]PKInfo, error) {
	// 1. 先获取pks 的名字
	submitQuery := fmt.Sprintf(sqlGetTablePk, tableName)
	qr, err := jc.execQuery(ctx, tableSchema, submitQuery)
	if err != nil {
		return nil, err
	}
	if len(qr.Named().Rows) == 0 {
		return nil, errors.New("the len of qr of getting pk info is 0")
	}
	var pkNames []string
	for _, row := range qr.Named().Rows {
		pkNames = append(pkNames, row["Column_name"].ToString())
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

func (jc *JobController) getIndexCount(tableSchema, tableName string) (indexCount int, err error) {
	query := fmt.Sprintf(sqlGetIndexCount, tableName)

	ctx := context.Background()
	qr, err := jc.execQuery(ctx, tableSchema, query)
	if err != nil {
		return 0, err
	}
	indexCount = len(qr.Named().Rows)
	if indexCount == 0 {
		return 0, errors.New("index count is 0")
	}
	return indexCount, nil
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
		newBatchID = fmt.Sprintf("%d-2", num)
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

func (args *JobArgs) initArgsByQueryResult(row sqltypes.RowNamedValues) {
	args.uuid = row["job_uuid"].ToString()
	args.tableSchema = row["table_schema"].ToString()
	args.table = row["table_name"].ToString()
	args.batchInfoTable = row["batch_info_table_name"].ToString()
	args.failPolicy = row["fail_policy"].ToString()
	args.status = row["status"].ToString()
	args.statusSetTime = row["status_set_time"].ToString()
	args.timeZone = row["time_zone"].ToString()

	batchInterval, _ := row["batch_interval_in_ms"].ToInt64()
	batchSize, _ := row["batch_size"].ToInt64()
	args.batchInterval = batchInterval
	args.batchSize = batchSize

	runningTimePeriodStart := row["running_time_period_start"].ToString()
	runningTimePeriodEnd := row["running_time_period_end"].ToString()
	runningTimePeriodTimeZone := row["running_time_period_time_zone"].ToString()

	args.timePeriodStart, args.timePeriodEnd = getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone)

}

func (jc *JobController) insertBatchInfoTableEntry(ctx context.Context, tableSchema, batchTableName, currentBatchID, batchSQL, countSQL, batchStartStr, batchEndStr string, batchSize int64) (err error) {
	insertBatchSQLWithTableName := fmt.Sprintf(sqlTemplateInsertBatchEntry, batchTableName)
	insertBatchSQLQuery, err := sqlparser.ParseAndBind(insertBatchSQLWithTableName,
		sqltypes.StringBindVariable(currentBatchID),
		sqltypes.StringBindVariable(batchSQL),
		sqltypes.StringBindVariable(countSQL),
		sqltypes.Int64BindVariable(batchSize),
		sqltypes.StringBindVariable(batchStartStr),
		sqltypes.StringBindVariable(batchEndStr))
	if err != nil {
		return err
	}
	_, err = jc.execQuery(ctx, tableSchema, insertBatchSQLQuery)
	if err != nil {
		return err
	}
	return nil
}

func (jc *JobController) insertJobEntry(jobUUID, sql, tableSchema, tableName, batchInfoTableSchema,
	batchInfoTable, jobStatus, statusSetTime, failPolicy, runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone string,
	timeGapInMs, batchSize int64) (err error) {

	runningTimePeriodStart = stripApostrophe(runningTimePeriodStart)
	runningTimePeriodEnd = stripApostrophe(runningTimePeriodEnd)
	runningTimePeriodTimeZone = stripApostrophe(runningTimePeriodTimeZone)
	if !isTimePeriodValid(runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodTimeZone) {
		return errors.New("check the format, the start and end should be like 'hh:mm:ss' and time zone should be like 'UTC[\\+\\-]\\d{2}:\\d{2}:\\d{2}'")
	}
	if runningTimePeriodTimeZone == "" {
		// 如果用户没有设置时区，则使用系统默认时区
		_, timeZoneOffset := time.Now().Zone()
		runningTimePeriodTimeZone = getTimeZoneStr(timeZoneOffset)
	}

	_, offset := time.Now().Zone()
	statusSetTimeTimeZone := getTimeZoneStr(offset)

	submitQuery, err := sqlparser.ParseAndBind(sqlDMLJobSubmit,
		sqltypes.StringBindVariable(jobUUID),
		sqltypes.StringBindVariable(sql),
		sqltypes.StringBindVariable(tableSchema),
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(batchInfoTableSchema),
		sqltypes.StringBindVariable(batchInfoTable),
		sqltypes.StringBindVariable(jobStatus),
		sqltypes.StringBindVariable(statusSetTime),
		sqltypes.StringBindVariable(statusSetTimeTimeZone),
		sqltypes.StringBindVariable(failPolicy),
		sqltypes.StringBindVariable(runningTimePeriodStart),
		sqltypes.StringBindVariable(runningTimePeriodEnd),
		sqltypes.StringBindVariable(runningTimePeriodTimeZone),
		sqltypes.Int64BindVariable(timeGapInMs),
		sqltypes.Int64BindVariable(batchSize))

	if err != nil {
		return err
	}
	_, err = jc.execQuery(jc.ctx, "", submitQuery)
	if err != nil {
		return err
	}
	return nil
}

func stripApostrophe(s string) string {
	if len(s) > 1 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

func getTimeZoneStr(offset int) string {
	if offset == 0 {
		return "UTC"
	}
	timeZone := "UTC"
	if offset > 0 {
		timeZone = timeZone + "+"
	} else {
		timeZone = timeZone + "-"
		offset = -offset
	}

	duration := time.Second * time.Duration(offset)
	startTime := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
	resultTime := startTime.Add(duration)
	timeFormat := "15:04:05"
	offsetStr := resultTime.Format(timeFormat)
	timeZone += offsetStr
	return timeZone
}

func getTimeZoneOffset(timeZoneStr string) (int, error) {
	if timeZoneStr == "UTC" {
		return 0, nil
	}
	if len(timeZoneStr) < 4 {
		return 0, fmt.Errorf("timeZoneStr is in wrong format")
	}
	neg := false
	if timeZoneStr[:3] != "UTC" {
		return 0, fmt.Errorf("timeZoneStr is in wrong format")
	}
	if timeZoneStr[3] != '-' && timeZoneStr[3] != '+' {
		return 0, fmt.Errorf("timeZoneStr is in wrong format")
	}
	if timeZoneStr[3] == '-' {
		neg = true
	}
	timeZoneStr = timeZoneStr[4:]
	parts := strings.Split(timeZoneStr, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("timeZoneStr is in wrong format")
	}

	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}

	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}

	second, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, err
	}

	if hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return 0, fmt.Errorf("timeZoneStr is in wrong format")
	}

	offset := hour*3600 + minute*60 + second
	if neg {
		offset = -offset
	}

	return offset, nil
}

// tableExists checks if a given table exists.
func (jc *JobController) tableExists(ctx context.Context, tableSchema, tableName string) (bool, error) {
	tableName = strings.ReplaceAll(tableName, `_`, `\_`)
	parsed := sqlparser.BuildParsedQuery(sqlShowTablesLike, tableName)
	rs, err := jc.execQuery(ctx, tableSchema, parsed.Query)
	if err != nil {
		return false, err
	}
	row := rs.Named().Row()
	return (row != nil), nil
}

func nonTransactionalDMLToGCUUID(uuid string) string {
	return strings.Replace(uuid, "-", "", -1)
}
