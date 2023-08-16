/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2020 The Vitess Authors.

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

package repltracker

import (
	"sync"
	"time"
	"vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/stats"

	mysqlctl "vitess.io/vitess/go/vt/mysqlctl"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var replicationLagSeconds = stats.NewGauge("replicationLagSec", "replication lag in seconds")

type poller struct {
	mysqld mysqlctl.MysqlDaemon

	mu           sync.Mutex
	lag          time.Duration
	timeRecorded time.Time
}

func (p *poller) InitDBConfig(mysqld mysqlctl.MysqlDaemon) {
	p.mysqld = mysqld
}

func (p *poller) Status() (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	status, err := p.mysqld.ReplicationStatus()
	if err != nil {
		return 0, err
	}

	// If replication is not currently running or we don't know what the lag is -- most commonly
	// because the replica mysqld is in the process of trying to start replicating from its source
	// but it hasn't yet reached the point where it can calculate the seconds_behind_master
	// value and it's thus NULL -- then we will estimate the lag ourselves using the last seen
	// value + the time elapsed since.
	if !status.Healthy() || status.ReplicationLagUnknown {
		if p.timeRecorded.IsZero() {
			return 0, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "replication is not running")
		}
		return time.Since(p.timeRecorded) + p.lag, nil
	}

	p.lag = time.Duration(status.ReplicationLagSeconds) * time.Second
	p.timeRecorded = time.Now()
	replicationLagSeconds.Set(int64(p.lag.Seconds()))
	return p.lag, nil
}

func (p *poller) GtidExecuted() (mysql.Position, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	position, err := p.mysqld.PrimaryPosition()
	if err != nil {
		return mysql.Position{}, err
	}
	return position, nil
}

func (p *poller) MysqlThreads() (*query.MysqlThreadsStats, error) {
	var mysqld, ok = p.mysqld.(*mysqlctl.Mysqld)
	if !ok {
		return nil, nil
	}
	status, err := mysqld.ThreadsStatus()
	if err != nil {
		return nil, err
	}
	var Threads query.MysqlThreadsStats
	for threadsType, quant := range status {
		number := int64(quant)
		switch threadsType {
		case mysql.ThreadsConnected:
			Threads.Connected = number
		case mysql.ThreadsCached:
			Threads.Cached = number
		case mysql.ThreadsCreated:
			Threads.Created = number
		case mysql.ThreadsRunning:
			Threads.Running = number
		default:
		}
	}
	return &Threads, nil
}
