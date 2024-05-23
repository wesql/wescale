/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wesql/wescale/go/sqltypes"
	vtrpcpb "github.com/wesql/wescale/go/vt/proto/vtrpc"
	"github.com/wesql/wescale/go/vt/sqlparser"
	"github.com/wesql/wescale/go/vt/vterrors"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/throttle"
)

var throttleTicks int64
var throttleInit sync.Once

func initThrottleTicker() {
	throttleInit.Do(func() {
		go func() {
			tick := time.NewTicker(time.Duration(throttleCheckInterval) * time.Millisecond)
			defer tick.Stop()
			for range tick.C {
				atomic.AddInt64(&throttleTicks, 1)
			}
		}()
	})
}

func (jc *JobController) ThrottleApp(uuid, expireString string, ratioLiteral *sqlparser.Literal) (expireAtStr string, ratio float64, err error) {
	duration, ratio, err := jc.validateThrottleParams(expireString, ratioLiteral)
	if err != nil {
		return "", 0, err
	}
	if err := jc.lagThrottler.CheckIsReady(); err != nil {
		return "", 0, err
	}
	expireAt := time.Now().Add(duration)
	_ = jc.lagThrottler.ThrottleApp(uuid, expireAt, ratio)
	expireAtStr = expireAt.String()
	return expireAtStr, ratio, err
}

// ratio: 1 means totally throttled
// expireString example: "300ms", "-1.5h", "2h45m"
func (jc *JobController) ThrottleJob(uuid, throttleDuration, throttleRatio string) (result *sqltypes.Result, err error) {
	ratioLiteral := sqlparser.NewDecimalLiteral(throttleRatio)
	emptyResult := &sqltypes.Result{}
	expireAtStr, ratio, err := jc.ThrottleApp(uuid, throttleDuration, ratioLiteral)
	if err != nil {
		return emptyResult, err
	}

	query, err := sqlparser.ParseAndBind(sqlDMLJobUpdateThrottleInfo,
		sqltypes.Float64BindVariable(ratio),
		sqltypes.StringBindVariable(expireAtStr),
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

func (jc *JobController) requestThrottle(uuid string) (throttleCheckOK bool) {
	if jc.lastSuccessfulThrottle >= atomic.LoadInt64(&throttleTicks) {
		// if last check was OK just very recently there is no need to check again
		return true
	}
	ctx := context.Background()
	// dml-job" prefix is added to the app name.
	// This allows throttling all DML jobs by throttle "dml-job" app
	appName := "dml-job:" + uuid
	throttleCheckFlags := &throttle.CheckFlags{}
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

func setDefaultValForThrottleParam(throttleDuration, throttleRatio string) (string, string) {
	if throttleDuration[0] == '\'' && throttleDuration[len(throttleDuration)-1] == '\'' {
		throttleDuration = throttleDuration[1 : len(throttleDuration)-1]
	}
	if throttleRatio == "" {
		throttleRatio = "1"
	}
	return throttleDuration, throttleRatio
}
