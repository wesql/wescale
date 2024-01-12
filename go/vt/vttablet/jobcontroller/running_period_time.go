/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"errors"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

// 外部调用的函数需要保证格式正确，即isTimePeriodValid函数返回true
func getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodEndTimeZone string) (*time.Time, *time.Time) {
	if runningTimePeriodStart != "" && runningTimePeriodEnd != "" {
		// 在submit job时或setRunningTimePeriod时，已经对格式进行了检查，因此这里不会出现错误
		periodStartTime, _ := time.Parse(time.TimeOnly, runningTimePeriodStart)
		periodEndTime, _ := time.Parse(time.TimeOnly, runningTimePeriodEnd)
		// 由于用户只提供了时间部分，因此需要将日期部分用当天的时间补齐。同时加上时区
		timeZoneOffset, _ := getTimeZoneOffset(runningTimePeriodEndTimeZone)
		timeZone := time.FixedZone(runningTimePeriodEndTimeZone, timeZoneOffset)
		currentTime := time.Now()
		periodStartTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), periodStartTime.Hour(), periodStartTime.Minute(), periodStartTime.Second(), periodStartTime.Nanosecond(), timeZone)
		periodEndTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), periodEndTime.Hour(), periodEndTime.Minute(), periodEndTime.Second(), periodEndTime.Nanosecond(), timeZone)
		// 如果EndTime早于startTime的时间，则EndTime的日期部分用明天的日期补齐
		if periodEndTime.Before(periodStartTime) {
			periodEndTime = periodEndTime.Add(24 * time.Hour)
		}
		return &periodStartTime, &periodEndTime
	}
	return nil, nil
}

func isTimePeriodValid(startTime, endTime, timeZone string) bool {
	// 合法情况：1.三者全为空 2.三者全不为空 3.时间不为空，时区为空
	validCase := (startTime == "" && endTime == "" && timeZone == "") ||
		(startTime != "" && endTime != "" && timeZone != "") ||
		(startTime != "" && endTime != "" && timeZone == "")
	if !validCase {
		return false
	}
	// 分别检查其内容的合法性
	if startTime != "" && endTime != "" {
		_, err := time.Parse(time.TimeOnly, startTime)
		if err != nil {
			return false
		}
		_, err = time.Parse(time.TimeOnly, endTime)
		if err != nil {
			return false
		}
	}

	if timeZone != "" {
		// 如果用户设置了时区，则检查是否是合法的时区
		_, err := getTimeZoneOffset(timeZone)
		if err != nil {
			return false
		}
	}

	return true
}

func (jc *JobController) SetRunningTimePeriod(uuid, startTime, endTime, timeZone string) (*sqltypes.Result, error) {
	var emptyResult = &sqltypes.Result{}

	if !isTimePeriodValid(startTime, endTime, timeZone) {
		return emptyResult, errors.New("check the format, the start and end should be like 'hh:mm:ss' and time zone should be like 'UTC[\\+\\-]\\d{2}:\\d{2}:\\d{2}'")
	}

	if timeZone == "" {
		// 如果用户没有设置时区，则使用系统默认时区
		_, timeZoneOffset := time.Now().Zone()
		timeZone = getTimeZoneStr(timeZoneOffset)
	}

	// 为了使得用户设置的运维时间能够生效，需要先将该job设置为暂停状态
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status == runningStatus {
		return emptyResult, errors.New("the job is running now, pause it first")
	}

	// 往表中插入
	return jc.updateJobPeriodTime(jc.ctx, uuid, startTime, endTime, timeZone)
}
