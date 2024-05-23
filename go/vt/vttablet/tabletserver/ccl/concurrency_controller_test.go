/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ccl

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"context"

	"github.com/wesql/wescale/go/streamlog"
	"github.com/wesql/wescale/go/vt/vterrors"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "github.com/wesql/wescale/go/vt/proto/vtrpc"
)

func resetVariables(txs *ConcurrencyController) {
	txs.waits.ResetAll()
	txs.waitsDryRun.ResetAll()
	txs.queueExceeded.ResetAll()
	txs.queueExceededDryRun.ResetAll()
	txs.globalQueueExceeded.Reset()
	txs.globalQueueExceededDryRun.Reset()
}

func NewConcurrentControllerForTest(maxGlobalQueueSize int, dryRun bool) *ConcurrencyController {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, "ConcurrencyControllerTest")
	concurrencyControllerMaxGlobalQueueSize = maxGlobalQueueSize
	concurrencyControllerDryRun = dryRun
	txs := New(env.Exporter())
	resetVariables(txs)
	return txs
}

func TestConcurrencyController_NoHotRow(t *testing.T) {
	txs := NewConcurrentControllerForTest(1, false)
	resetVariables(txs)

	q := txs.GetOrCreateQueue("t1 where1", 1, 5)
	//fmt.Println(q)
	//done, waited, err := q.Wait(context.Background(), []string{"t1"})
	done, waited, err := q.Wait(context.Background(), []string{"t1"})
	//done, waited, err := q.Wait(context.Background(), []string{"t1"})
	if err != nil {
		t.Error(err)
	}
	if waited {
		t.Error("non-parallel tx must never wait")
	}
	done()

	// No concurrency control was recoded.
	if err := testHTTPHandler(txs, 1, false); err != nil {
		t.Error(err)
	}
	// No transaction had to wait.
	if got, want := txs.waits.Counts()["t1"], int64(0); got != want {
		t.Errorf("wrong Waits variable: got = %v, want = %v", got, want)
	}
}

func TestConcurrencyControllerRedactDebugUI(t *testing.T) {
	streamlog.SetRedactDebugUIQueries(true)
	defer func() {
		streamlog.SetRedactDebugUIQueries(false)
	}()

	txs := NewConcurrentControllerForTest(1, false)
	resetVariables(txs)
	q := txs.GetOrCreateQueue("t1 where1", 1, 1)
	done, waited, err := q.Wait(context.Background(), []string{"t1"})
	if err != nil {
		t.Error(err)
	}
	if waited {
		t.Error("non-parallel tx must never wait")
	}
	done()

	// No concurrency control was recoded.
	if err := testHTTPHandler(txs, 1, true); err != nil {
		t.Error(err)
	}
	// No transaction had to wait.
	if got, want := txs.waits.Counts()["t1"], int64(0); got != want {
		t.Errorf("wrong Waits variable: got = %v, want = %v", got, want)
	}
}

func TestConcurrencyController(t *testing.T) {
	txs := NewConcurrentControllerForTest(3, false)
	resetVariables(txs)

	// tx1.
	q := txs.GetOrCreateQueue("t1 where1", 2, 1)
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("tx1 must never wait: %v", waited1)
	}

	// tx2 (gets queued and must wait).
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		done2, waited2, err2 := q.Wait(context.Background(), []string{"t1"})
		if err2 != nil {
			t.Error(err2)
		}
		if !waited2 {
			t.Errorf("tx2 must wait: %v", waited2)
		}
		if got, want := txs.waits.Counts()["t1"], int64(1); got != want {
			t.Errorf("variable not incremented: got = %v, want = %v", got, want)
		}

		done2()
	}()
	// Wait until tx2 is waiting before we try tx3.
	if err := waitForPending(txs, "t1 where1", 2); err != nil {
		t.Error(err)
	}

	// tx3 (gets rejected because it would exceed the local Queue).
	_, _, err3 := q.Wait(context.Background(), []string{"t1"})
	if got, want := vterrors.Code(err3), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Errorf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err3.Error(), "concurrency control protection: too many queued transactions (2 >= 2)"; got != want {
		t.Errorf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}

	done1()
	// tx2 must have been unblocked.
	wg.Wait()

	if txs.queues["t1 where1"] != nil {
		t.Error("Queue object was not deleted after last transaction")
	}

	// 2 transactions were recorded.
	if err := testHTTPHandler(txs, 2, false); err != nil {
		t.Error(err)
	}
	// 1 of them had to wait.
	if got, want := txs.waits.Counts()["t1"], int64(1); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
	// 1 (the third one) was rejected because the Queue was exceeded.
	if got, want := txs.queueExceeded.Counts()["t1"], int64(1); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
}

func TestConcurrencyController_ConcurrentTransactions(t *testing.T) {
	// Allow up to 2 concurrent transactions per concurrency control.
	txs := NewConcurrentControllerForTest(3, false)
	resetVariables(txs)

	// tx1.
	q := txs.GetOrCreateQueue("t1 where1", 3, 2)
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("tx1 must never wait: %v", waited1)
	}

	// tx2.
	done2, waited2, err2 := q.Wait(context.Background(), []string{"t1"})
	if err2 != nil {
		t.Error(err1)
	}
	if waited2 {
		t.Errorf("tx2 must not wait: %v", waited1)
	}

	// tx3 (gets queued and must wait).
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		done3, waited3, err3 := q.Wait(context.Background(), []string{"t1"})
		if err3 != nil {
			t.Error(err3)
		}
		if !waited3 {
			t.Errorf("tx3 must wait: %v", waited2)
		}
		if got, want := txs.waits.Counts()["t1"], int64(1); got != want {
			t.Errorf("variable not incremented: got = %v, want = %v", got, want)
		}

		done3()
	}()

	// Wait until tx3 is waiting before we finish tx2 and unblock tx3.
	if err := waitForPending(txs, "t1 where1", 3); err != nil {
		t.Error(err)
	}
	// Finish tx2 before tx1 to test that the "finish-order" does not matter.
	// Unblocks tx3.
	done2()
	// Wait for tx3 to finish.
	wg.Wait()
	// Finish tx1 to delete the Queue object.
	done1()

	if txs.queues["t1 where1"] != nil {
		t.Error("Queue object was not deleted after last transaction")
	}

	// 3 transactions were recorded.
	if err := testHTTPHandler(txs, 3, false); err != nil {
		t.Error(err)
	}
	// 1 of them had to wait.
	if got, want := txs.waits.Counts()["t1"], int64(1); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
}

func waitForPending(txs *ConcurrencyController, key string, i int) error {
	start := time.Now()
	for {
		got, want := txs.Pending(key), i
		if got == want {
			return nil
		}

		if time.Since(start) > 10*time.Second {
			return fmt.Errorf("wait for ConcurrencyController.Pending() = %d timed out: got = %v, want = %v", i, got, want)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func testHTTPHandler(txs *ConcurrencyController, count int, redacted bool) error {
	req, err := http.NewRequest("GET", "/path-is-ignored-in-test", nil)
	if err != nil {
		return err
	}
	rr := httptest.NewRecorder()
	txs.ServeHTTP(rr, req)

	if got, want := rr.Code, http.StatusOK; got != want {
		return fmt.Errorf("wrong status code: got = %v, want = %v", got, want)
	}

	if redacted {
		if !strings.Contains(rr.Body.String(), "/debug/ccl has been redacted for your protection") {
			return fmt.Errorf("expected /debug/ccl to be redacted")
		}
		return nil
	}

	want := fmt.Sprintf(`Length: 1
%d: t1 where1
`, count)
	if count == 0 {
		want = `Length: 0
`
	}
	if got := rr.Body.String(); got != want {
		return fmt.Errorf("wrong content: got = \n%v\n want = \n%v", got, want)
	}

	return nil
}

// TestConcurrencyControllerCancel runs 4 pending transactions.
// tx1 and tx2 are allowed to run concurrently while tx3 and tx4 are queued.
// tx3 will get canceled and tx4 will be unblocked once tx1 is done.
func TestConcurrencyControllerCancel(t *testing.T) {
	txs := NewConcurrentControllerForTest(4, false)
	resetVariables(txs)

	// tx3 and tx4 will record their number once they're done waiting.
	txDone := make(chan int)
	q := txs.GetOrCreateQueue("t1 where1", 4, 2)
	// tx1.
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("tx1 must never wait: %v", waited1)
	}
	// tx2.
	done2, waited2, err2 := q.Wait(context.Background(), []string{"t1"})
	if err2 != nil {
		t.Error(err2)
	}
	if waited2 {
		t.Errorf("tx2 must not wait: %v", waited2)
	}

	// tx3 (gets queued and must wait).
	ctx3, cancel3 := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, _, err3 := q.Wait(ctx3, []string{"t1"})
		if err3 != context.Canceled {
			t.Error(err3)
		}

		txDone <- 3
	}()
	// Wait until tx3 is waiting before we try tx4.
	if err := waitForPending(txs, "t1 where1", 3); err != nil {
		t.Error(err)
	}

	// tx4 (gets queued and must wait as well).
	wg.Add(1)
	go func() {
		defer wg.Done()

		done4, waited4, err4 := q.Wait(context.Background(), []string{"t1"})
		if err4 != nil {
			t.Error(err4)
		}
		if !waited4 {
			t.Errorf("tx4 must have waited: %v", waited4)
		}

		txDone <- 4

		done4()
	}()
	// Wait until tx4 is waiting before we start to cancel tx3.
	if err := waitForPending(txs, "t1 where1", 4); err != nil {
		t.Error(err)
	}

	// Cancel tx3.
	cancel3()
	if got := <-txDone; got != 3 {
		t.Errorf("tx3 should have been unblocked after the cancel: %v", got)
	}
	// Finish tx1.
	done1()
	// Wait for tx4.
	if got := <-txDone; got != 4 {
		t.Errorf("wrong tx was unblocked after tx1: %v", got)
	}
	wg.Wait()
	// Finish tx2 (the last transaction) which will delete the Queue object.
	done2()

	if txs.queues["t1 where1"] != nil {
		t.Error("Queue object was not deleted after last transaction")
	}

	// 4 total transactions get recorded.
	if err := testHTTPHandler(txs, 4, false); err != nil {
		t.Error(err)
	}
	// 2 of them had to wait.
	if got, want := txs.waits.Counts()["t1"], int64(2); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
}

// TestConcurrencyControllerDryRun verifies that the dry-run mode does not serialize
// the two concurrent transactions for the same key.
func TestConcurrencyControllerDryRun(t *testing.T) {
	txs := NewConcurrentControllerForTest(2, true)
	resetVariables(txs)
	q := txs.GetOrCreateQueue("t1 where1", 1, 1)
	// tx1.
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("first transaction must never wait: %v", waited1)
	}

	// tx2 (would wait and exceed the local Queue).
	done2, waited2, err2 := q.Wait(context.Background(), []string{"t1"})
	if err2 != nil {
		t.Error(err2)
	}
	if waited2 {
		t.Errorf("second transaction must never wait in dry-run mode: %v", waited2)
	}
	if got, want := txs.waitsDryRun.Counts()["t1"], int64(2); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
	if got, want := txs.queueExceededDryRun.Counts()["t1"], int64(1); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}

	// tx3 (would wait and exceed the global Queue).
	done3, waited3, err3 := q.Wait(context.Background(), []string{"t1"})
	if err3 != nil {
		t.Error(err3)
	}
	if waited3 {
		t.Errorf("any transaction must never wait in dry-run mode: %v", waited3)
	}
	if got, want := txs.waitsDryRun.Counts()["t1"], int64(3); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}
	if got, want := txs.globalQueueExceededDryRun.Get(), int64(1); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}

	if got, want := txs.Pending("t1 where1"), 3; got != want {
		t.Errorf("wrong number of pending transactions: got = %v, want = %v", got, want)
	}

	done1()
	done2()
	done3()

	if txs.queues["t1 where1"] != nil {
		t.Error("Queue object was not deleted after last transaction")
	}

	if err := testHTTPHandler(txs, 3, false); err != nil {
		t.Error(err)
	}
}

// TestConcurrencyControllerGlobalQueueOverflow shows that the global Queue can exceed
// its limit without rejecting errors. This is the case when all transactions
// are the first one for their row range.
// This is done on purpose to avoid that a too low global Queue limit would
// reject transactions although they may succeed within the txpool constraints
// and RPC deadline.
func TestConcurrencyControllerGlobalQueueOverflow(t *testing.T) {
	txs := NewConcurrentControllerForTest(1, false)

	// tx1.
	q := txs.GetOrCreateQueue("t1 where1", 1, 1)
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("first transaction must never wait: %v", waited1)
	}

	// tx2.
	q2 := txs.GetOrCreateQueue("t1 where2", 1, 1)
	done2, waited2, err2 := q2.Wait(context.Background(), []string{"t1"})
	assert.Error(t, err2, "concurrency control protection: too many queued transactions")
	if waited2 {
		t.Errorf("second transaction for different row range must not wait: %v", waited2)
	}
	assert.Nil(t, done2)

	// tx3 (same row range as tx1).
	_, _, err3 := q.Wait(context.Background(), []string{"t1"})
	if got, want := vterrors.Code(err3), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Errorf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err3.Error(), "concurrency control protection: too many global queued transactions (1 >= 1)"; got != want {
		t.Errorf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}
	if got, want := txs.globalQueueExceeded.Get(), int64(2); got != want {
		t.Errorf("variable not incremented: got = %v, want = %v", got, want)
	}

	done1()
}

func TestConcurrencyControllerPending(t *testing.T) {
	txs := NewConcurrentControllerForTest(1, false)
	if got, want := txs.Pending("t1 where1"), 0; got != want {
		t.Errorf("there should be no pending transaction: got = %v, want = %v", got, want)
	}
}

func BenchmarkConcurrencyController_NoHotRow(b *testing.B) {
	txs := NewConcurrentControllerForTest(1, false)
	q := txs.GetOrCreateQueue("t1 where1", 1, 5)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		done, waited, err := q.Wait(context.Background(), []string{"t1"})
		if err != nil {
			b.Error(err)
		}
		if waited {
			b.Error("non-parallel tx must never wait")
		}
		done()
	}
}

func TestConcurrencyController_DenyAll_global(t *testing.T) {
	txs := NewConcurrentControllerForTest(0, false)
	q := txs.GetOrCreateQueue("t1 where1", 1, 1)
	done, waited, err := q.Wait(context.Background(), []string{"t1"})
	if err == nil {
		t.Error("expected error")
		return
	}
	assert.Nil(t, done)
	if waited {
		t.Error("no transaction must ever wait")
	}

	if got, want := vterrors.Code(err), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Errorf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err.Error(), "concurrency control protection: too many global queued transactions (0 >= 0)"; got != want {
		t.Errorf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}
}

func TestConcurrencyController_DenyAll_queue_size(t *testing.T) {
	txs := NewConcurrentControllerForTest(1, false)
	q := txs.GetOrCreateQueue("t1 where1", 0, 1)
	done, waited, err := q.Wait(context.Background(), []string{"t1"})
	if err == nil {
		t.Error("expected error")
		return
	}
	assert.Nil(t, done)
	if waited {
		t.Error("no transaction must ever wait")
	}

	if got, want := vterrors.Code(err), vtrpcpb.Code_RESOURCE_EXHAUSTED; got != want {
		t.Errorf("wrong error code: got = %v, want = %v", got, want)
	}
	if got, want := err.Error(), "concurrency control protection: too many queued transactions (0 >= 0)"; got != want {
		t.Errorf("transaction rejected with wrong error: got = %v, want = %v", got, want)
	}
}

func TestConcurrencyController_DenyAll_concurrency(t *testing.T) {
	txs := NewConcurrentControllerForTest(1, false)
	q := txs.GetOrCreateQueue("t1 where1", 1, 0)
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	done, waited, err := q.Wait(ctx, []string{"t1"})
	if err == nil {
		t.Error("expected error")
		return
	}
	assert.Nil(t, done)
	assert.True(t, waited)

	if got, want := vterrors.Code(err), vtrpcpb.Code_DEADLINE_EXCEEDED; got != want {
		t.Errorf("wrong error code: got = %v, want = %v", got, want)
	}
}

// TestConcurrencyControllerWaitingRequest runs 3 pending transactions.
// tx1 and tx2 are allowed to run concurrently while tx3 are queued.
// tx3 will get canceled.
// this is to test q.waitingRequest work correctly.
func TestConcurrencyControllerWaitingRequest(t *testing.T) {
	txs := NewConcurrentControllerForTest(4, false)
	resetVariables(txs)

	txDone := make(chan int)
	q := txs.GetOrCreateQueue("t1 where1", 4, 2)
	// tx1.
	done1, waited1, err1 := q.Wait(context.Background(), []string{"t1"})
	if err1 != nil {
		t.Error(err1)
	}
	if waited1 {
		t.Errorf("tx1 must never wait: %v", waited1)
	}
	// tx2.
	done2, waited2, err2 := q.Wait(context.Background(), []string{"t1"})
	if err2 != nil {
		t.Error(err2)
	}
	if waited2 {
		t.Errorf("tx2 must not wait: %v", waited2)
	}

	// tx3 (gets queued and must wait).
	ctx3, cancel3 := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, _, err3 := q.Wait(ctx3, []string{"t1"})
		if err3 != context.Canceled {
			t.Error(err3)
		}

		txDone <- 3
	}()
	// Wait until tx3 is waiting before we try tx4.
	if err := waitForPending(txs, "t1 where1", 3); err != nil {
		t.Error(err)
	}

	// tx3 is waiting.
	assert.Equal(t, 1, q.waitingRequest.Len())

	// Cancel tx3.
	cancel3()
	if got := <-txDone; got != 3 {
		t.Errorf("tx3 should have been unblocked after the cancel: %v", got)
	}

	// onTheFlySize should be 2 because currently tx1 and tx2 are running.
	assert.Equal(t, 2, q.onTheFlySize)
	// Though tx3 has been canceled, its ch (which is already closed) still remains in the list.
	assert.Equal(t, 1, q.waitingRequest.Len())

	// Finish tx1.
	done1()
	// onTheFlySize should be 1 because currently only tx2 is running.
	assert.Equal(t, 1, q.onTheFlySize)
	// the ch remains by tx3 should be 1 removed.
	assert.Equal(t, 0, q.waitingRequest.Len())

	// Finish tx2 (the last transaction) which will delete the Queue object.
	done2()
	if txs.queues["t1 where1"] != nil {
		t.Error("Queue object was not deleted after last transaction")
	}
}

func startATransactionShouldNotWait(t *testing.T, q *Queue, txNum int, tables []string) DoneFunc {
	done, waited, err := q.Wait(context.Background(), tables)
	if err != nil {
		t.Error(err)
	}
	if waited {
		t.Errorf("tx %d must never wait: %v", txNum, waited)
	}
	return done
}

// doneFunc is the return parameter.
func startATransactionShouldWait(t *testing.T, q *Queue, txNum int, txGetResource chan int, txReleased *int, expectedTxReleased int, doneFunc *DoneFunc) {
	go func() {
		done, waited, err := q.Wait(context.Background(), []string{"t1"})
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, true, waited)
		// tx6 get unblocked only after 3 transactions finish and no new transaction starts.
		if txReleased != nil {
			assert.Equal(t, expectedTxReleased, *txReleased)
		}
		*doneFunc = done
		txGetResource <- txNum
	}()
	// sleep for a while to make sure tx is waiting before we do the next thing.
	time.Sleep(100 * time.Millisecond)
}

func TestConcurrencyControllerResizeQueueSimple(t *testing.T) {
	txs := NewConcurrentControllerForTest(10, false)
	resetVariables(txs)

	go func() {
		timeout := 1 * time.Second
		time.Sleep(timeout)
		t.Fatal("Test case failed due to timeout")
	}()
	txs.GetOrCreateQueue("t1 where1", 1, 1)
	txs.GetOrCreateQueue("t1 where1", 0, 0)
}

func TestConcurrencyControllerResizeQueue(t *testing.T) {
	txs := NewConcurrentControllerForTest(10, false)
	resetVariables(txs)

	txGetResource := make(chan int)
	q := txs.GetOrCreateQueue("t1 where1", 5, 1)
	// tx1 should not wait.
	done1 := startATransactionShouldNotWait(t, q, 1, []string{"t1"})

	// tx2 should wait, but it cancels the context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		_, _, err2 := q.Wait(ctx2, []string{"t1"})
		assert.NotNil(t, err2)
	}()
	cancel2()

	// tx3 should wait.
	var done3 DoneFunc
	startATransactionShouldWait(t, q, 3, txGetResource, nil, 0, &done3)

	// ---- 1. new maxConcurrency is greater than on the fly plus waiting num (tx1 is on the fly, and tx3 is waiting). ----
	q.txs.mu.Lock()
	q.resizeQueueLocked(5, 3)
	q.txs.mu.Unlock()

	// tx3 should unblock.
	if got := <-txGetResource; got != 3 {
		t.Errorf("tx3 should have been unblocked after the resize: %v", got)
	}

	assert.Equal(t, 2, q.onTheFlySize)
	assert.Equal(t, 0, q.waitingRequest.Len())

	// tx4 should not wait.
	done4 := startATransactionShouldNotWait(t, q, 4, []string{"t1"})

	// tx5 should wait.
	txReleased := 0
	var done5 DoneFunc
	startATransactionShouldWait(t, q, 5, txGetResource, &txReleased, 2, &done5)

	// ---- 2. new maxConcurrency is smaller than on the fly plus waiting num (tx1, tx3, tx4 is on the fly, and tx5 is waiting). ----
	q.txs.mu.Lock()
	q.resizeQueueLocked(5, 2)
	q.txs.mu.Unlock()

	// tx6 should wait.
	var done6 DoneFunc
	startATransactionShouldWait(t, q, 6, txGetResource, &txReleased, 3, &done6)

	// we finish tx1 and tx3, and tx5 will unblock.
	txReleased = 2
	done1()
	done3()
	if got := <-txGetResource; got != 5 {
		t.Errorf("tx5 should have been unblocked after tx1 and tx3 finish: %v", got)
	}

	assert.Equal(t, 2, q.onTheFlySize)
	// now only tx6 is waiting.
	assert.Equal(t, 1, q.waitingRequest.Len())

	// we finish tx4, and tx6 will unblock.
	txReleased = 3
	done4()
	if got := <-txGetResource; got != 6 {
		t.Errorf("tx6 should have been unblocked after tx4 finish: %v", got)
	}

	assert.Equal(t, 2, q.onTheFlySize)
	assert.Equal(t, 0, q.waitingRequest.Len())

	// tx7 should wait.
	var done7 DoneFunc
	startATransactionShouldWait(t, q, 7, txGetResource, nil, 0, &done7)

	// tx8 should wait.
	var done8 DoneFunc
	startATransactionShouldWait(t, q, 8, txGetResource, &txReleased, 1, &done8)

	// ---- 3. new maxConcurrency is greater than on the fly but smaller on the fly than plus waiting num (tx5, tx6 is on the fly, and tx7, tx8 is waiting). ----
	q.txs.mu.Lock()
	q.resizeQueueLocked(5, 3)
	q.txs.mu.Unlock()

	// tx7 should unblock.
	if got := <-txGetResource; got != 7 {
		t.Errorf("tx7 should have been unblocked after the resize: %v", got)
	}
	assert.Equal(t, 3, q.onTheFlySize)
	// tx8 is still waiting.
	assert.Equal(t, 1, q.waitingRequest.Len())

	// clear txReleased and recount
	txReleased = 1
	done5()

	// tx8 should unblock.
	if got := <-txGetResource; got != 8 {
		t.Errorf("tx8 should have been unblocked after tx5 finish: %v", got)
	}
	// tx6, tx7, tx8 are on the fly.
	assert.Equal(t, 3, q.onTheFlySize)
	assert.Equal(t, 0, q.waitingRequest.Len())

	// ---- 4. change MaxQueueSize. ----
	q.txs.mu.Lock()
	q.resizeQueueLocked(3, 3)
	q.txs.mu.Unlock()
	_, waited, err := q.Wait(context.Background(), []string{"t1"})
	assert.Equal(t, false, waited)
	assert.NotNil(t, err)

	q.txs.mu.Lock()
	q.resizeQueueLocked(4, 3)
	q.txs.mu.Unlock()
	// tx9 should wait.
	var done9 DoneFunc
	startATransactionShouldWait(t, q, 9, txGetResource, &txReleased, 1, &done9)

	// clear txReleased and recount
	txReleased = 1
	done6()
	// tx9 should unblock.
	if got := <-txGetResource; got != 9 {
		t.Errorf("tx9 should have been unblocked after tx6 finish: %v", got)
	}
	assert.Equal(t, 3, q.onTheFlySize)
	assert.Equal(t, 0, q.waitingRequest.Len())
	done7()
	done8()
	done9()
	assert.Equal(t, 0, q.onTheFlySize)
	assert.Equal(t, 0, q.waitingRequest.Len())
	assert.Nil(t, txs.queues["t1"])
}
