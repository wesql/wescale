package tabletserver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func TestContinueAction(t *testing.T) {
	action := &ContinueAction{}
	qre := &QueryExecutor{}
	assert.NoError(t, action.BeforeExecution(qre))
	assert.Equal(t, &ActionExecutionResponse{
		FireNext: true,
	}, action.AfterExecution(qre, nil))
	assert.NoError(t, action.SetParams(""))
	assert.Nil(t, action.GetRule())
}

func TestFailAction(t *testing.T) {
	qr := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFail)

	action := &FailAction{
		Rule:   qr,
		Action: rules.QRFail,
	}
	qre := &QueryExecutor{}
	assert.ErrorContains(t, action.BeforeExecution(qre), "disallowed due to rule")
	assert.Equal(t, &ActionExecutionResponse{
		FireNext: true,
	}, action.AfterExecution(qre, nil))
	assert.NoError(t, action.SetParams(""))
	assert.NotNil(t, action.GetRule())
}

func TestFailRetryAction(t *testing.T) {
	qr := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRFailRetry)

	action := &FailRetryAction{
		Rule:   qr,
		Action: rules.QRFailRetry,
	}
	qre := &QueryExecutor{}
	err := action.BeforeExecution(qre)
	assert.ErrorContains(t, err, "disallowed due to rule")
	// check if the error is retried: Code_FAILED_PRECONDITION
	errCode := vterrors.Code(err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, errCode)

	assert.Equal(t, &ActionExecutionResponse{
		FireNext: true,
	}, action.AfterExecution(qre, nil))
	assert.NoError(t, action.SetParams(""))
	assert.NotNil(t, action.GetRule())
}

func TestConcurrencyControlAction(t *testing.T) {
	qr := rules.NewQueryRule("ruleDescription", "test_rule", rules.QRConcurrencyControl)

	action := &ConcurrencyControlAction{
		Rule:           qr,
		Action:         rules.QRConcurrencyControl,
		MaxQueueSize:   2,
		MaxConcurrency: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db := setUpQueryExecutorTest(t)
	defer db.Close()
	tsv := newTestTabletServer(ctx, noFlags, db)
	qre := newTestQueryExecutor(ctx, tsv, "select * from t1 where a = :a and b = :b", 0)

	// test with no concurrency control
	assert.NoError(t, action.BeforeExecution(qre))

	wg := &sync.WaitGroup{}
	wg.Add(2)
	// test with concurrency control
	go func() {
		defer wg.Done()
		timeOutErr := action.BeforeExecution(qre)
		assert.EqualError(t, timeOutErr, "context deadline exceeded")
	}()

	// test with concurrency control and max queue size
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		maxQueueSizeErr := action.BeforeExecution(qre)
		assert.EqualError(t, maxQueueSizeErr, "concurrency control protection: too many queued transactions (2 >= 2)")
	}()

	wg.Wait()

	{
		timeOutErr := action.BeforeExecution(qre)
		assert.EqualError(t, timeOutErr, "context deadline exceeded")
	}

	assert.Equal(t, &ActionExecutionResponse{
		FireNext: true,
	}, action.AfterExecution(qre, nil))

	assert.NoError(t, action.BeforeExecution(qre))

	assert.Equal(t, &ActionExecutionResponse{
		FireNext: true,
	}, action.AfterExecution(qre, nil))
}

func TestConcurrencyControlActionSetParams(t *testing.T) {
	action := &ConcurrencyControlAction{}
	params := `{"max_queue_size": 2, "max_concurrency": 1}`
	assert.NoError(t, action.SetParams(params))
	assert.Equal(t, 2, action.MaxQueueSize)
	assert.Equal(t, 1, action.MaxConcurrency)
}
