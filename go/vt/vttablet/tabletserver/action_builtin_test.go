package tabletserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	vtrpcpb "github.com/wesql/wescale/go/vt/proto/vtrpc"
	"github.com/wesql/wescale/go/vt/vterrors"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/rules"
)

func TestContinueAction(t *testing.T) {
	action := &ContinueAction{}
	qre := &QueryExecutor{}
	resp := action.BeforeExecution(qre)
	assert.NoError(t, resp.Err)
	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))
	assert.NoError(t, action.SetParams(nil))
	assert.Nil(t, action.GetRule())
}

func TestFailAction(t *testing.T) {
	qr := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFail)

	action := &FailAction{
		Rule:   qr,
		Action: rules.QRFail,
	}
	qre := &QueryExecutor{}
	resp := action.BeforeExecution(qre)
	assert.ErrorContains(t, resp.Err, "disallowed due to rule")
	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))
	assert.NoError(t, action.SetParams(nil))
	assert.NotNil(t, action.GetRule())
}

func TestFailRetryAction(t *testing.T) {
	qr := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRFailRetry)

	action := &FailRetryAction{
		Rule:   qr,
		Action: rules.QRFailRetry,
	}
	qre := &QueryExecutor{}
	resp := action.BeforeExecution(qre)
	assert.ErrorContains(t, resp.Err, "disallowed due to rule")
	// check if the error is retried: Code_FAILED_PRECONDITION
	errCode := vterrors.Code(resp.Err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, errCode)

	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))
	assert.NoError(t, action.SetParams(nil))
	assert.NotNil(t, action.GetRule())
}

func TestBufferActionNoTimeout(t *testing.T) {
	ruleCancelCtx, ruleCancelFunc := context.WithTimeout(context.Background(), 1*time.Second)
	defer ruleCancelFunc()
	qr := rules.NewActiveBufferedTableQueryRule(ruleCancelCtx, "d1.t1", "desc")

	action := &BufferAction{
		Rule:   qr,
		Action: rules.QRBuffer,
	}
	qre := &QueryExecutor{
		ctx: context.Background(),
	}
	resp := action.BeforeExecution(qre)
	assert.NoError(t, resp.Err)
}

func TestBufferActionTimeout(t *testing.T) {
	ruleCancelCtx, ruleCancelFunc := context.WithTimeout(context.Background(), 12*time.Second)
	defer ruleCancelFunc()
	qr := rules.NewActiveBufferedTableQueryRule(ruleCancelCtx, "d1.t1", "desc")

	action := &BufferAction{
		Rule:   qr,
		Action: rules.QRBuffer,
	}
	qre := &QueryExecutor{
		ctx: context.Background(),
	}
	resp := action.BeforeExecution(qre)
	assert.ErrorContains(t, resp.Err, "buffer timeout")
	// check if the error is retried: Code_FAILED_PRECONDITION
	errCode := vterrors.Code(resp.Err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, errCode)

	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))
	assert.NoError(t, action.SetParams(nil))
	assert.NotNil(t, action.GetRule())
}

func TestConcurrencyControlAction(t *testing.T) {
	qr := rules.NewActiveQueryRule("ruleDescription", "test_rule", rules.QRConcurrencyControl)

	action := &ConcurrencyControlAction{
		Rule:   qr,
		Action: rules.QRConcurrencyControl,
		Args:   &ConcurrencyControlActionArgs{MaxQueueSize: 2, MaxConcurrency: 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, "select * from t1 where a = :a and b = :b", 0)

	// test with no concurrency control
	resp := action.BeforeExecution(qre)
	assert.NoError(t, resp.Err)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	// test with concurrency control
	go func() {
		defer wg.Done()
		timeoutResp := action.BeforeExecution(qre)
		assert.EqualError(t, timeoutResp.Err, "context deadline exceeded")
	}()

	// test with concurrency control and max queue size
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		maxQueueSizeResp := action.BeforeExecution(qre)
		assert.EqualError(t, maxQueueSizeResp.Err, "concurrency control protection: too many queued transactions (2 >= 2)")
	}()

	wg.Wait()

	{
		timeoutResp := action.BeforeExecution(qre)
		assert.EqualError(t, timeoutResp.Err, "context deadline exceeded")
	}

	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))

	resp2 := action.BeforeExecution(qre)
	assert.NoError(t, resp2.Err)

	assert.Equal(t, &ActionExecutionResponse{}, action.AfterExecution(qre, nil, nil))
}

func TestConcurrencyControlActionSetParams(t *testing.T) {
	action := &ConcurrencyControlAction{}
	params := `max_queue_size=2; max_concurrency=1`
	args, err := action.ParseParams(params)
	assert.NoError(t, err)
	assert.NoError(t, action.SetParams(args))
	assert.Equal(t, 2, action.Args.MaxQueueSize)
	assert.Equal(t, 1, action.Args.MaxConcurrency)

	// max_queue_size type invalid
	params = `max_queue_size=2.5; max_concurrency=1`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_concurrency type invalid
	params = `max_queue_size=2; max_concurrency="1"`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_concurrency < 0, invalid
	params = `max_queue_size=-1; max_concurrency=1`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_concurrency < 0, invalid
	params = `max_queue_size=2; max_concurrency=-2`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_queue_size < max_concurrency, invalid
	params = `max_queue_size=2; max_concurrency=3`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_concurrency = 0 and max_queue_size != 0, invalid
	params = `max_queue_size=2; max_concurrency=0`
	_, err = action.ParseParams(params)
	assert.NotNil(t, err)

	// max_concurrency = 0 and max_queue_size = 0, valid
	params = `max_queue_size=0; max_concurrency=0`
	args, err = action.ParseParams(params)
	assert.NoError(t, err)
	assert.NoError(t, action.SetParams(args))
	assert.Equal(t, 0, action.Args.MaxQueueSize)
	assert.Equal(t, 0, action.Args.MaxConcurrency)

	// max_concurrency = -1 and max_queue_size = 0, valid
	params = `max_queue_size=0; max_concurrency=-1`
	args, err = action.ParseParams(params)
	assert.NoError(t, err)
	assert.NoError(t, action.SetParams(args))
	assert.Equal(t, 0, action.Args.MaxQueueSize)
	assert.Equal(t, -1, action.Args.MaxConcurrency)
}
