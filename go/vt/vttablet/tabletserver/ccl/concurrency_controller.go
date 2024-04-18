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
	"container/list"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/logutil"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	concurrencyControllerDryRun             = false
	concurrencyControllerMaxGlobalQueueSize = 10000000
)

func registerCclFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&concurrencyControllerDryRun, "concurrency_controller_dry_run", concurrencyControllerDryRun, "Dry run mode for the concurrency controller")
	fs.IntVar(&concurrencyControllerMaxGlobalQueueSize, "concurrency_controller_max_global_queue_size", concurrencyControllerMaxGlobalQueueSize, "Maximum number of transactions that can be queued globally")
}

func init() {
	servenv.OnParseFor("vttablet", registerCclFlags)
}

// ConcurrencyController serializes incoming transactions which target the same row range
// Additional transactions are queued and woken up in arrival order.
//
// This implementation has some parallels to the sync2.Consolidator class.
// However, there are many substantial differences:
//   - Results are not shared between queued transactions.
//   - Only one waiting transaction and not all are notified when the current one
//     has finished.
//   - Waiting transactions are woken up in FIFO order.
//   - Waiting transactions are unblocked if their context is done.
//   - Both the local Queue (per row range) and global Queue (whole process) are
//     limited to avoid that queued transactions can consume the full capacity
//     of vttablet. This is important if the capaciy is finite. For example, the
//     number of RPCs in flight could be limited by the RPC subsystem.
type ConcurrencyController struct {
	*sync2.ConsolidatorCache

	// Immutable fields.
	dryRun             bool
	maxGlobalQueueSize int

	// waits stores how many times a transaction was queued because another
	// transaction was already in flight.
	// The key of the map is the table name of the query.
	//
	// waitsDryRun is similar as "waits": In dry-run mode it records how many
	// transactions would have been queued.
	// The key of the map is the table name and WHERE clause.
	//
	// queueExceeded counts per table how many transactions were rejected because
	// the max Queue size per row (range) was exceeded.
	//
	// queueExceededDryRun counts in dry-run mode how many transactions would have
	// been rejected due to exceeding the max Queue size per row (range).
	//
	// globalQueueExceeded is the same as queueExceeded but for the global Queue.
	waits, waitsDryRun, queueExceeded, queueExceededDryRun *stats.CountersWithSingleLabel
	globalQueueExceeded, globalQueueExceededDryRun         *stats.Counter

	log                          *logutil.ThrottledLogger
	logDryRun                    *logutil.ThrottledLogger
	logWaitsDryRun               *logutil.ThrottledLogger
	logQueueExceededDryRun       *logutil.ThrottledLogger
	logGlobalQueueExceededDryRun *logutil.ThrottledLogger

	mu                sync.Mutex
	queues            map[string]*Queue
	currentGlobalSize int
}

// New returns a ConcurrencyController object.
func New(exporter *servenv.Exporter) *ConcurrencyController {
	return &ConcurrencyController{
		ConsolidatorCache:  sync2.NewConsolidatorCache(1000),
		dryRun:             concurrencyControllerDryRun,
		maxGlobalQueueSize: concurrencyControllerMaxGlobalQueueSize,
		waits: exporter.NewCountersWithSingleLabel(
			"ConcurrencyControllerWaits",
			"Number of times a transaction was queued because another transaction was already in flight",
			"table_name"),
		waitsDryRun: exporter.NewCountersWithSingleLabel(
			"ConcurrencyControllerWaitsDryRun",
			"Dry run number of transactions that would've been queued",
			"table_name"),
		queueExceeded: exporter.NewCountersWithSingleLabel(
			"ConcurrencyControllerQueueExceeded",
			"Number of transactions that were rejected because the max Queue size per row range was exceeded",
			"table_name"),
		queueExceededDryRun: exporter.NewCountersWithSingleLabel(
			"ConcurrencyControllerQueueExceededDryRun",
			"Dry-run Number of transactions that were rejected because the max Queue size was exceeded",
			"table_name"),
		globalQueueExceeded: exporter.NewCounter(
			"ConcurrencyControllerGlobalQueueExceeded",
			"Number of transactions that were rejected on the global Queue because of exceeding the max Queue size per row range"),
		globalQueueExceededDryRun: exporter.NewCounter(
			"ConcurrencyControllerGlobalQueueExceededDryRun",
			"Dry-run stats for ConcurrencyControllerGlobalQueueExceeded"),
		log:                          logutil.NewThrottledLogger("ConcurrencyController", 5*time.Second),
		logDryRun:                    logutil.NewThrottledLogger("ConcurrencyController DryRun", 5*time.Second),
		logWaitsDryRun:               logutil.NewThrottledLogger("ConcurrencyController Waits DryRun", 5*time.Second),
		logQueueExceededDryRun:       logutil.NewThrottledLogger("ConcurrencyController QueueExceeded DryRun", 5*time.Second),
		logGlobalQueueExceededDryRun: logutil.NewThrottledLogger("ConcurrencyController GlobalQueueExceeded DryRun", 5*time.Second),
		queues:                       make(map[string]*Queue),
	}
}

// DoneFunc is returned by Wait() and must be called by the caller.
type DoneFunc func()

// GetOrCreateQueue creates a new Queue for the given key if it does not exist.
func (txs *ConcurrencyController) GetOrCreateQueue(key string, maxQueueSize, maxConcurrency int) *Queue {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	_, ok := txs.queues[key]
	if !ok {
		txs.queues[key] = newQueue(key, txs, maxQueueSize, maxConcurrency)
	}
	q := txs.queues[key]
	if q.maxQueueSize != maxQueueSize || q.maxConcurrency != maxConcurrency {
		q.resizeQueue(maxQueueSize, maxConcurrency)
	}
	return txs.queues[key]
}

// Wait blocks if another transaction for the same range is already in flight.
// It returns when this transaction has its turn.
// "done" is != nil if err == nil and must be called once the transaction is
// done and the next waiting transaction can be unblocked.
// "waited" is true if Wait() had to wait for other transactions.
// "err" is not nil if a) the context is done or b) a Queue limit was reached.
func (q *Queue) Wait(ctx context.Context, tables []string) (done DoneFunc, waited bool, err error) {
	q.txs.mu.Lock()
	defer q.txs.mu.Unlock()

	waited, err = q.lockLocked(ctx, q.key, tables)
	if err != nil {
		if waited {
			// Waiting failed early e.g. due a canceled context and we did NOT get the
			// slot. Call "done" now because we don'txs return it to the caller.
			q.unlockLocked(q.key, false /* returnSlot */)
		}
		return nil, waited, err
	}
	return func() { q.unlock(q.key) }, waited, nil
}

// lockLocked queues this transaction. It will unblock immediately if this
// transaction is the first in the Queue or when it acquired a slot.
// The method has the suffix "Locked" to clarify that "txs.mu" must be locked.
func (q *Queue) lockLocked(ctx context.Context, key string, tables []string) (bool, error) {
	txs := q.txs

	if txs.currentGlobalSize >= txs.maxGlobalQueueSize {
		if txs.dryRun {
			txs.globalQueueExceededDryRun.Add(1)
			txs.logGlobalQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d)", txs.currentGlobalSize, txs.maxGlobalQueueSize)
		} else {
			txs.globalQueueExceeded.Add(1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"concurrency control protection: too many global queued transactions (%d >= %d)", txs.currentGlobalSize, txs.maxGlobalQueueSize)
		}
	}

	if q.size >= q.maxQueueSize {
		if txs.dryRun {
			for _, table := range tables {
				txs.queueExceededDryRun.Add(table, 1)
			}
			txs.logQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d)", q.size, q.maxQueueSize)
		} else {
			for _, table := range tables {
				txs.queueExceeded.Add(table, 1)
			}
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"concurrency control protection: too many queued transactions (%d >= %d)", q.size, q.maxQueueSize)
		}
	}

	txs.currentGlobalSize++
	q.size++
	q.count++
	if q.size > q.max {
		q.max = q.size
	}
	// Publish the number of waits at /debug/hotrows.
	txs.Record(key)

	if txs.dryRun {
		for _, table := range tables {
			txs.waitsDryRun.Add(table, 1)
		}
		txs.logWaitsDryRun.Warningf("Would have queued BeginExecute RPC for row (range): '%v' because another transaction to the same range is already in progress.", key)
		return false, nil
	}

	var ch chan struct{}
	if q.onTheFlySize < q.maxConcurrency {
		q.onTheFlySize++
		return false, nil
	}

	ch = make(chan struct{})
	q.waitingRequest.PushBack(ch)

	// Unlock before the wait and relock before returning because our caller
	// Wait() holds the lock and assumes it still has it.
	txs.mu.Unlock()
	defer txs.mu.Lock()

	// Blocking wait for the next available slot.
	for _, table := range tables {
		txs.waits.Add(table, 1)
	}

	select {
	case ch <- struct{}{}:
		return true, nil
	case <-ctx.Done():
		close(ch)
		return true, ctx.Err()
	}

}

func (q *Queue) unlock(key string) {
	q.txs.mu.Lock()
	defer q.txs.mu.Unlock()

	q.unlockLocked(key, true)
}

func (q *Queue) unlockLocked(key string, returnSlot bool) {
	txs := q.txs
	q.size--
	txs.currentGlobalSize--

	if q.size == 0 {
		// This is the last transaction in flight.
		q.onTheFlySize--
		delete(txs.queues, key)

		if q.max > 1 {
			logMsg := fmt.Sprintf("%v simultaneous transactions (%v in total) would have been queued.", q.max, q.count)
			if txs.dryRun {
				txs.logDryRun.Infof(logMsg)
			} else {
				txs.log.Infof(logMsg)
			}
		}

		// Return early because the Queue "q" for this "key" will not be used any
		// more.
		// We intentionally skip returning the last slot and closing the
		// "availableSlots" channel because it is not required by Go.
		return
	}

	// Give up slot by removing ourselves from the channel.
	// Wakes up the next queued transaction.

	if txs.dryRun {
		// Dry-run did not acquire a slot in the first place.
		return
	}

	if !returnSlot {
		// We did not acquire a slot in the first place e.g. due to a canceled context.
		return
	}

	if q.onTheFlySize > q.maxConcurrency {
		// Due to resizing, we may have more than maxConcurrency transactions in flight.
		q.onTheFlySize--
		return
	}

	hasReleasedOne := false
	for q.waitingRequest.Len() > 0 {
		front := q.waitingRequest.Front()
		q.waitingRequest.Remove(front)
		ch := front.Value.(chan struct{})
		// this should never block
		_, ok := <-ch
		if ok {
			hasReleasedOne = true
			break
		}
		// !ok means ch is closed due to timeout, and the ch will not be removed from the list by the requester
		// because the doneFunc returned to it is nil, so here we should remove closed ch, and find the next valid ch to remove.
	}

	if !hasReleasedOne {
		q.onTheFlySize--
	}
}

// Pending returns the number of queued transactions (including the ones which
// are currently in flight.)
func (txs *ConcurrencyController) Pending(key string) int {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	q, ok := txs.queues[key]
	if !ok {
		return 0
	}
	return q.size
}

// ServeHTTP lists the most recent, cached queries and their count.
func (txs *ConcurrencyController) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if streamlog.GetRedactDebugUIQueries() {
		response.Write([]byte(`
	<!DOCTYPE html>
	<html>
	<body>
	<h1>Redacted</h1>
	<p>/debug/ccl has been redacted for your protection</p>
	</body>
	</html>
		`))
		return
	}

	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	items := txs.Items()
	response.Header().Set("Content-Type", "text/plain")
	if items == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(items))))
	for _, v := range items {
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Count, v.Query)))
	}
}

// Queue represents the local Queue for a particular row (range).
//
// Note that we don't use a dedicated Queue structure for all waiting
// transactions. Instead, we leverage that Go routines waiting for a channel
// are woken up in the order they are queued up. The "availableSlots" field is
// said channel which has n free slots (for the number of concurrent
// transactions which can access the tx pool). All queued transactions are
// competing for these slots and try to add themselves to the channel.
type Queue struct {
	// NOTE: The following fields are guarded by ConcurrencyController.mu.
	key string
	// maxQueueSize is the maximum number of transactions which can be queued, including the ones that are in flight.
	maxQueueSize int
	// maxConcurrency is the maximum number of transactions which can be executed concurrently.
	maxConcurrency int

	// size counts how many transactions are currently queued/in flight (includes
	// the transactions which are not waiting.)
	size int
	// count is the same as "size", but never gets decremented.
	count int
	// max is the max of "size", i.e. the maximum number of transactions which
	// were simultaneously queued
	max int

	// onTheFlySize is the number of transactions which are currently in flight,
	// it may be greater than maxConcurrency due to queue resizing.
	onTheFlySize int

	// waitingRequest is a list of channels representing transactions which are currently queued.
	waitingRequest list.List

	txs *ConcurrencyController
}

func newQueue(key string, txs *ConcurrencyController, maxQueueSize, maxConcurrency int) *Queue {
	return &Queue{
		key:            key,
		maxQueueSize:   maxQueueSize,
		maxConcurrency: maxConcurrency,
		size:           0,
		count:          0,
		max:            0,
		txs:            txs,
	}
}

func (q *Queue) resizeQueue(newMaxQueueSize, newMaxConcurrency int) {
	// lock to make sure there are no resource requesting and releasing during resizing progress.
	q.txs.mu.Lock()
	defer q.txs.mu.Unlock()

	q.maxQueueSize = newMaxQueueSize

	if newMaxConcurrency > q.maxConcurrency {
		delta := newMaxConcurrency - q.maxConcurrency
		for delta > 0 && q.waitingRequest.Len() > 0 {
			front := q.waitingRequest.Front()
			q.waitingRequest.Remove(front)
			ch := front.Value.(chan struct{})
			_, ok := <-ch
			if ok {
				delta--
				q.onTheFlySize++
			}
		}
	}
	// if newMaxConcurrency <= maxConcurrency, just set q.MaxConcurrency to new value,
	// which will lead q.onTheFlySize greater than q.MaxConcurrency,
	// we don't need to do anything more.
	q.maxConcurrency = newMaxConcurrency
}
