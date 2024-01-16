/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestIsTimePeriodValid(t *testing.T) {
	type testCase struct {
		startTime string
		endTime   string
		timeZone  string
		result    bool
	}

	testCases := []testCase{
		// valid
		{startTime: "09:00:00", endTime: "17:00:00", timeZone: "", result: true},
		{startTime: "09:00:00", endTime: "17:00:00", timeZone: "UTC+08:00:00", result: true},
		{startTime: "", endTime: "", timeZone: "", result: true},
		// invalid
		{startTime: "25:00:00", endTime: "17:00:00", timeZone: "", result: false},
		{startTime: "09:00:00", endTime: "25:00:00", timeZone: "", result: false},
		{startTime: "", endTime: "17:00:00", timeZone: "", result: false},
		{startTime: "17:00:00", endTime: "", timeZone: "", result: false},
		{startTime: "", endTime: "", timeZone: "UTC+08:00:00", result: false},
		{startTime: "'09:00:00'", endTime: "'17:00:00'", timeZone: "'UTC+08:00:00'", result: false},
	}

	for _, test := range testCases {
		t.Run("TestIsTimePeriodValid", func(t *testing.T) {
			if isTimePeriodValid(test.startTime, test.endTime, test.timeZone) != test.result {
				t.Errorf("isTimePeriodValid(%v, %v, %v) should return %v", test.startTime, test.endTime, test.timeZone, test.result)
			}
		})
	}
}

func TestTimeZone(t *testing.T) {
	type testCase struct {
		startTime  string
		endTime    string
		timeZone   string
		startTime2 string
		endTime2   string
		timeZone2  string
	}

	testCases := []testCase{
		{startTime: "09:00:00", endTime: "17:00:00", timeZone: "UTC+08:00:00",
			startTime2: "09:10:00", endTime2: "17:10:00", timeZone2: "UTC+08:10:00"},
		{startTime: "09:00:00", endTime: "17:00:00", timeZone: "UTC+08:00:00",
			startTime2: "08:45:00", endTime2: "16:45:00", timeZone2: "UTC+07:45:00"},
	}

	for _, test := range testCases {
		t.Run("TestIsTimePeriodValid", func(t *testing.T) {
			startTime, endTime := getRunningPeriodTime(test.startTime, test.endTime, test.timeZone)
			startTime2, endTime2 := getRunningPeriodTime(test.startTime2, test.endTime2, test.timeZone2)
			assert.Equal(t, startTime.Unix(), startTime2.Unix())
			assert.Equal(t, endTime.Unix(), endTime2.Unix())
		})
	}
}
