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

// the function caller should make sure that runningTimePeriodStart and runningTimePeriodEnd is valid
func getRunningPeriodTime(runningTimePeriodStart, runningTimePeriodEnd, runningTimePeriodEndTimeZone string) (*time.Time, *time.Time) {
	if runningTimePeriodStart != "" && runningTimePeriodEnd != "" {
		periodStartTime, _ := time.Parse(time.TimeOnly, runningTimePeriodStart)
		periodEndTime, _ := time.Parse(time.TimeOnly, runningTimePeriodEnd)
		timeZoneOffset, _ := getTimeZoneOffset(runningTimePeriodEndTimeZone)
		timeZone := time.FixedZone(runningTimePeriodEndTimeZone, timeZoneOffset)
		// we should get year, month and day in correct timeZone
		currentTimeInTimeZone := time.Now().In(timeZone)
		periodStartTime = time.Date(currentTimeInTimeZone.Year(), currentTimeInTimeZone.Month(), currentTimeInTimeZone.Day(), periodStartTime.Hour(), periodStartTime.Minute(), periodStartTime.Second(), periodStartTime.Nanosecond(), timeZone)
		periodEndTime = time.Date(currentTimeInTimeZone.Year(), currentTimeInTimeZone.Month(), currentTimeInTimeZone.Day(), periodEndTime.Hour(), periodEndTime.Minute(), periodEndTime.Second(), periodEndTime.Nanosecond(), timeZone)
		// if EndTime is earlier than startTime, we add 24 hour s to EndTime
		if periodEndTime.Before(periodStartTime) {
			periodEndTime = periodEndTime.Add(24 * time.Hour)
		}
		return &periodStartTime, &periodEndTime
	}
	return nil, nil
}

func isTimePeriodValid(startTime, endTime, timeZone string) bool {
	// valid case：
	// 1. all of them are empty string
	// 2. all of them are not empty string
	// 3. time is not empty but time zone is
	validCase := (startTime == "" && endTime == "" && timeZone == "") ||
		(startTime != "" && endTime != "" && timeZone != "") ||
		(startTime != "" && endTime != "" && timeZone == "")
	if !validCase {
		return false
	}
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
		// use system time zone if user didn't set it
		_, timeZoneOffset := time.Now().Zone()
		timeZone = getTimeZoneStr(timeZoneOffset)
	}

	// user should pause the job before setting running time period
	status, err := jc.getStrJobInfo(jc.ctx, uuid, "status")
	if err != nil {
		return emptyResult, err
	}
	if status == RunningStatus {
		return emptyResult, errors.New("the job is running now, pause it first")
	}

	qr, err := jc.updateJobPeriodTime(jc.ctx, uuid, startTime, endTime, timeZone)
	jc.notifyJobManager()
	return qr, err
}
