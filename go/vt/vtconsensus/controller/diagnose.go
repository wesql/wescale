/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

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

package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtconsensus/db"
)

var pingTabletTimeout = 2 * time.Second

func init() {
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.DurationVar(&pingTabletTimeout, "ping_tablet_timeout", 2*time.Second, "time to wait when we ping a tablet")
	})
}

// DiagnoseType is the types of Diagnose result
type DiagnoseType string

const (
	// DiagnoseTypeError represents an DiagnoseTypeError status
	DiagnoseTypeError DiagnoseType = "error"
	// DiagnoseTypeHealthy represents everything is DiagnoseTypeHealthy
	DiagnoseTypeHealthy = "Healthy"
	// DiagnoseTypeUnreachablePrimary represents the primary tablet is unreachable
	DiagnoseTypeUnreachablePrimary = "UnreachablePrimary"
	// DiagnoseTypeWrongPrimaryTablet represents the primary tablet is incorrect based on wesql-server leader
	DiagnoseTypeWrongPrimaryTablet = "WrongPrimaryTablet"
	// DiagnoseTypeUnreachableConsensusLeader represents the wesql-server consensus leader is unreachable
	DiagnoseTypeUnreachableConsensusLeader = "UnreachableConsensusLeader"
	// DiagnoseTypeMissingConsensusLeader represents miss consensus leader on wesql-server
	DiagnoseTypeMissingConsensusLeader = "MissingConsensusLeader"
)

// ScanAndRepairShard scans a particular shard by first Diagnose the shard with info from consensusShard
// and then repair the problem if the shard is unhealthy
func (shard *ConsensusShard) ScanAndRepairShard(ctx context.Context) {
	shard.logger.Infof("ScanAndRepairShard diagnose %v status", formatKeyspaceShard(shard.KeyspaceShard))
	status, err := shard.Diagnose(ctx)
	if err != nil {
		shard.logger.Infof("failed to diagnose %v/%v error: %v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, err)
	}
	// We are able to get Diagnose without error.
	// Note: all the recovery function should first try to grab a shard level lock
	// and check the trigger conditions before doing anything. This is to avoid
	// other VTConsensus instance try to do the same thing
	shard.logger.Infof("%v status is %v", formatKeyspaceShard(shard.KeyspaceShard), status)
	if _, err := shard.Repair(ctx, status); err != nil {
		shard.logger.Errorf("failed to ScanAndRepairShard repair %v: %v", status, err)
	}
}

func (shard *ConsensusShard) Diagnose(ctx context.Context) (DiagnoseType, error) {
	shard.Lock()
	defer shard.Unlock()
	diagnoseResult, err := shard.diagnoseLocked(ctx)
	shard.shardStatusCollector.recordDiagnoseResult(diagnoseResult)
	shard.populateVTConsensusStatusLocked()
	if diagnoseResult != DiagnoseTypeHealthy {
		shard.logger.Warningf("VTConsensus diagnose shard as unhealthy for %s/%s:\nresult=%v \ninstances=%v \nprimary=%v \nprimary_tablet=%v \nproblematics=%v \nunreachables=%v,\n%v",
			shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard,
			shard.shardStatusCollector.status.DiagnoseResult,
			shard.shardStatusCollector.status.Instances,
			shard.shardStatusCollector.status.Primary,
			shard.primaryTabletAlias(),
			shard.shardStatusCollector.status.Problematics,
			shard.shardStatusCollector.status.Unreachables,
			shard.sqlConsensusView.ToString())
	}
	return diagnoseResult, err
}

func (shard *ConsensusShard) diagnoseLocked(ctx context.Context) (DiagnoseType, error) {
	// 1. fetch consensus leader instance from wesql-server consensus global view.
	// TODO add fast path: directly fetch leader local view by primary tablet, if primary tablet exist.
	err := shard.refreshSQLConsensusView()
	if err != nil {
		if err == errUnreachableLeaderMySQL {
			return DiagnoseTypeUnreachableConsensusLeader, errUnreachableLeaderMySQL
		}
		if err == errMissingConsensusLeader {
			return DiagnoseTypeMissingConsensusLeader, errMissingConsensusLeader
		}
		return DiagnoseTypeError, vterrors.Wrap(err, "fail to refreshSQLConsensusView")
	}

	// 2. available leader found, check if the primary tablet is available.
	diagnoseType, err := shard.checkPrimaryTablet(ctx)

	return diagnoseType, err
}

// checkPrimaryTablet checks if the primary tablet is available.
func (shard *ConsensusShard) checkPrimaryTablet(ctx context.Context) (DiagnoseType, error) {
	host, port, isOnline := shard.sqlConsensusView.GetPrimary()
	// if we failed to find leader for wesql-server, maybe consensus cluster no ready or unhealthy.
	if !isOnline || host == "" || port == 0 {
		shard.logger.Infof("wesql-server consensus no Leader %v:%v", host, port)
		return DiagnoseTypeMissingConsensusLeader, errMissingConsensusLeader
	}

	// find primary tablet in the vitess
	primary := shard.findShardPrimaryTablet()
	// If we failed to find primary for shard, it mostly means we are initializing the shard and all tablets are replica.
	// If the primary and consensus leader mismatch, then logging and return DiagnoseTypeWrongPrimaryTablet.
	if primary == nil || (host != primary.instanceKey.Hostname) || (port != primary.instanceKey.Port) {
		shard.logger.Infof("vitess primary and wesql-server leader mismatch ,currently leader is %v:%v", host, port)
		return DiagnoseTypeWrongPrimaryTablet, errWrongPrimaryTablet
	}

	// we have a primary tablet available, check if primary tablet is reachable
	if !shard.instanceReachable(ctx, primary) {
		shard.logger.Infof("Failed to reach primary tablet that is running with leader on %v:%v", host, port)
		return DiagnoseTypeUnreachablePrimary, errUnreachablePrimaryTablet
	}

	return DiagnoseTypeHealthy, nil
}

func (shard *ConsensusShard) instanceReachable(ctx context.Context, instance *consensusInstance) bool {
	pingCtx, cancel := context.WithTimeout(context.Background(), pingTabletTimeout)
	defer cancel()
	c := make(chan error, 1)
	// tmc.Ping create grpc client connection first without timeout via dial
	// then call the grpc endpoint using the context with timeout
	// this is problematic if the host is really unreachable, we have to wait the
	// all the retries inside grpc.dial with exponential backoff
	go func() { c <- shard.tmc.Ping(pingCtx, instance.tablet) }()
	select {
	case <-pingCtx.Done():
		shard.logger.Errorf("Ping abort timeout %v", pingTabletTimeout)
		return false
	case err := <-c:
		if err != nil {
			shard.logger.Errorf("Ping error host=%v: %v", instance.instanceKey.Hostname, err)
		}
		return err == nil
	}
}

// findShardPrimaryTablet returns the primary for the shard
// it is either based on shard info from global topo or based on tablet types
// from local topo
func (shard *ConsensusShard) findShardPrimaryTablet() *consensusInstance {
	var primaryInstance *consensusInstance
	for _, instance := range shard.instances {
		if shard.PrimaryAlias == instance.alias {
			return instance
		}
	}
	return primaryInstance
}

func (shard *ConsensusShard) primaryTabletAlias() string {
	primary := shard.findShardPrimaryTablet()
	if primary == nil {
		return "UNKNOWN"
	}
	return primary.alias
}

func (collector *shardStatusCollector) recordDiagnoseResult(result DiagnoseType) {
	collector.Lock()
	defer collector.Unlock()
	collector.status.DiagnoseResult = result
}

func (collector *shardStatusCollector) recordUnreachables(instance *consensusInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Unreachables {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Unreachables = append(collector.status.Unreachables, instance.alias)
}

func (collector *shardStatusCollector) clear() {
	collector.Lock()
	defer collector.Unlock()
	collector.status.Unreachables = nil
	collector.status.Problematics = nil
}

// recordProblematics records the problematic instances
func (collector *shardStatusCollector) recordProblematics(instance *consensusInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Problematics {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Problematics = append(collector.status.Problematics, instance.alias)
}

func formatKeyspaceShard(keyspaceShard *topo.KeyspaceShard) string {
	return fmt.Sprintf("%v/%v", keyspaceShard.Keyspace, keyspaceShard.Shard)
}

func unreachableError(err error) bool {
	contains := []string{
		// "no such host"/"no route to host" is the error when a host is not reachalbe
		"no such host",
		"no route to host",
		// "connect: connection refused" is the error when a mysqld refused the connection
		"connect: connection refused",
		// "invalid mysql instance key" is the error when a tablet does not populate mysql hostname or port
		// this can happen if the tablet crashed. We keep them in the grShard.instances list to compute
		// quorum but consider it as an unreachable host.
		"invalid mysql instance key",
	}
	for _, k := range contains {
		if strings.Contains(err.Error(), k) {
			return true
		}
	}
	return false
}

// refreshSQLConsensusView hits all instances and renders a SQL group locally for later diagnoses
// the SQL group contains a list of "views" for the group from all the available nodes
func (shard *ConsensusShard) refreshSQLConsensusView() error {
	var leaderHost string
	var leaderPort int
	var leaderServerID int
	var leaderTerm int
	var leaderInstance *consensusInstance
	var wg sync.WaitGroup
	var mu sync.Mutex

	view := shard.dbAgent.NewConsensusGlobalView()

	// reset views in sql group
	shard.sqlConsensusView.clear()

	shard.shardStatusCollector.clear()
	// get local view from wesql-server instances by iterating through all the Vitess tablets.
	// If a Vitess tablet doesn't exist, the corresponding wesql-server instance won't be accessed.
	for _, instance := range shard.instances {
		wg.Add(1)
		go func(instance *consensusInstance) {
			defer wg.Done()
			localView, err := shard.dbAgent.FetchConsensusLocalView(instance.alias, instance.instanceKey, view)
			if err != nil {
				shard.shardStatusCollector.recordProblematics(instance)
				if unreachableError(err) {
					shard.shardStatusCollector.recordUnreachables(instance)
				}
				shard.logger.Errorf("%v error while fetch wesql_cluster_local: %v", instance.alias, err)
				// Only raise error if we failed to get any data from mysql
				// maybe some mysql node is not start.
				return
			}
			mu.Lock()
			defer mu.Unlock()

			// Currently only considers a leader to exist if the leader instance is directly
			// found from local view. Even if the leaderHostname is not empty in the local view of other follower,
			// it cannot be considered as the leader because the corresponding MySQLHost of the leader's tablet
			// cannot be found.Therefore, leader is only considered to be found when both the leader
			// and the corresponding tablet exist.
			if localView.Role == db.LEADER &&
				(nil == leaderInstance || leaderTerm < localView.CurrentTerm) {
				if leaderInstance != nil && leaderInstance != instance {
					shard.logger.Warningf("Dual leaders ard founded, old leader serverid %d: %v:%v, term is %v. new leader serverid %d: %v:%v, term is %v",
						leaderServerID,
						leaderHost,
						leaderPort,
						leaderTerm,
						localView.ServerID,
						instance.instanceKey.Hostname,
						instance.instanceKey.Port,
						localView.CurrentTerm)
				}
				leaderInstance = instance
				leaderHost = instance.instanceKey.Hostname
				leaderPort = instance.instanceKey.Port
				leaderServerID = localView.ServerID
				leaderTerm = localView.CurrentTerm
			}
		}(instance)
	}
	wg.Wait()

	// if local views ard found from instances, and leader instance is not nil,
	// then fetch global view from leader instance.
	if leaderInstance != nil {
		var err error
		shard.logger.Infof("get consensus leader serverid %d: %v:%v", leaderServerID, leaderHost, leaderPort)

		view.LeaderTabletMySQLHost = leaderHost
		view.LeaderTabletMySQLPort = leaderPort
		view.LeaderServerID = leaderServerID
		err = shard.dbAgent.FetchConsensusGlobalView(view)
		if err != nil {
			shard.shardStatusCollector.recordProblematics(leaderInstance)
			if unreachableError(err) {
				shard.shardStatusCollector.recordUnreachables(leaderInstance)
			}
			shard.logger.Errorf("%v error while fetch wesql_cluster_global: %v", leaderInstance.alias, err)
			shard.sqlConsensusView.recordView(view)
			return errUnreachableLeaderMySQL
		}
		if len(view.ResolvedMember) == 0 {
			shard.logger.Errorf("%v empty rows while fetch wesql_cluster_global, maybe switchover",
				leaderInstance.alias)
			shard.sqlConsensusView.recordView(nil)
			return errMissingConsensusLeader
		}
		shard.sqlConsensusView.recordView(view)
		return nil
	}

	shard.sqlConsensusView.recordView(view)
	return errMissingConsensusLeader
}
