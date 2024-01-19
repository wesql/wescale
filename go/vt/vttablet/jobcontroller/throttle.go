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

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
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
