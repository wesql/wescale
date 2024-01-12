/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import "testing"

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
	}

	for _, test := range testCases {
		t.Run("TestIsTimePeriodValid", func(t *testing.T) {
			if isTimePeriodValid(test.startTime, test.endTime, test.timeZone) != test.result {
				t.Errorf("isTimePeriodValid(%v, %v, %v) should return %v", test.startTime, test.endTime, test.timeZone, test.result)
			}
		})
	}
}
