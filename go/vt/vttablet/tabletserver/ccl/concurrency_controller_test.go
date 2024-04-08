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

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	//todo filter: remove maxQueueSize & maxConcurrency
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
