/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"errors"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

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

func (jc *JobController) SetRunningTimePeriod(uuid, startTime, endTime string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}
	ctx := context.Background()

	// 如果两个时间只有一个为空，则报错
	if (startTime == "" && endTime != "") || (startTime != "" && endTime == "") {
		return emptyResult, errors.New("the start time and end time must be both set or not")
	}

	status, err := jc.getStrJobInfo(ctx, uuid, "status")
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
