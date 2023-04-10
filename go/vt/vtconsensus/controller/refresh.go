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
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vtconsensus/inst"
	"vitess.io/vitess/go/vt/vtconsensus/log"
)

var (
	lockShardTimingsMs = stats.NewMultiTimings("lockShard", "time vtconsensus takes to lock the shard", []string{"operation", "success"})
)

// consensusInstance represents an instance that's running wesql-server
// it wraps a InstanceKey plus some tablet related information
type consensusInstance struct {
	instanceKey      *inst.InstanceKey
	tablet           *topodatapb.Tablet
	primaryTimeStamp time.Time
	alias            string
}

// ConsensusTopo is VTConsensus wrapper for topo server
type ConsensusTopo interface {
	GetShardNames(ctx context.Context, keyspace string) ([]string, error)
	GetShard(ctx context.Context, keyspace, shard string) (*topo.ShardInfo, error)
	GetTabletMapForShardByCell(ctx context.Context, keyspace, shard string, cells []string) (map[string]*topo.TabletInfo, error)
	LockShard(ctx context.Context, keyspace, shard, action string) (context.Context, func(*error), error)
}

// ConsensusTmcClient is VTConsensus wrapper for tmc client
type ConsensusTmcClient interface {
	ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType, semiSync bool) error
	Ping(ctx context.Context, tablet *topodatapb.Tablet) error
}

// ConsensusShard stores the information about a Vitess shard that's running wesql-server
type ConsensusShard struct {
	KeyspaceShard        *topo.KeyspaceShard
	cells                []string
	instances            []*consensusInstance
	PrimaryAlias         string
	shardStatusCollector *shardStatusCollector
	sqlConsensusView     *SQLConsensusView
	ts                   ConsensusTopo
	tmc                  ConsensusTmcClient
	dbAgent              db.Agent

	// Every ConsensusShard tracks a unlock function after it grab a topo lock for the shard
	// VTConsensus needs to release the topo lock before gracefully shutdown
	unlock func(*error)
	// mutex to protect unlock function access
	unlockMu sync.Mutex

	// configuration
	localDbPort int

	logger *log.Logger

	// lock prevents multiple go routine fights with each other
	sync.Mutex
}

// shardStatusCollector is used for collecting shard status
type shardStatusCollector struct {
	status *ShardStatus
	sync.Mutex
}

// ShardStatus is used for debugging purpose to get current status of a shard
type ShardStatus struct {
	Keyspace       string
	Shard          string
	Instances      []string
	Unreachables   []string
	Problematics   []string
	Primary        string
	DiagnoseResult DiagnoseType
}

func newShardStatusCollector(keyspace, shard string) *shardStatusCollector {
	return &shardStatusCollector{
		status: &ShardStatus{Keyspace: keyspace, Shard: shard},
	}
}

// NewConsensusShard creates a new ConsensusShard
func NewConsensusShard(
	keyspace, shard string,
	cells []string,
	tmc ConsensusTmcClient,
	ts ConsensusTopo,
	dbAgent db.Agent,
	localDbPort int) *ConsensusShard {
	consensusShard := &ConsensusShard{
		KeyspaceShard:        &topo.KeyspaceShard{Keyspace: keyspace, Shard: shard},
		cells:                cells,
		shardStatusCollector: newShardStatusCollector(keyspace, shard),
		tmc:                  tmc,
		ts:                   ts,
		dbAgent:              dbAgent,
		unlock:               nil,
		sqlConsensusView:     NewSQLConsensusView(keyspace, shard),
		localDbPort:          localDbPort,
		logger:               log.NewVTConsensusLogger(keyspace, shard),
	}
	return consensusShard
}

// RefreshTabletsInShardWithLock refreshes the shard instances and find primary tablet with a lock
func (shard *ConsensusShard) RefreshTabletsInShardWithLock(ctx context.Context) {
	instances, err := shard.refreshTabletsInShardInternal(ctx)
	if err == nil {
		// Take a per shard lock here when we actually refresh the data to avoid
		// race conditions between controller and repair tasks
		shard.Lock()
		shard.instances = instances
		shard.Unlock()
	}
	// get primary tablet from global topo server.
	primary, err := shard.refreshPrimaryShard(ctx)
	shard.Lock()
	defer shard.Unlock()
	// If the global topo server does not exist or there is no shard primary information,primary will be nil
	if err == nil && primary != "<nil>" {
		shard.PrimaryAlias = primary
		return
	}
	// set primary separately from instances so that if global topo is not available
	// still discover the new tablets from local cell
	shard.PrimaryAlias = shard.findPrimaryFromLocalCell()
}

// refreshTabletsInShardInternal refresh ks/shard instances.
func (shard *ConsensusShard) refreshTabletsInShardInternal(ctx context.Context) ([]*consensusInstance, error) {
	keyspace, shardName := shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard
	// get all tablets for a shard from topo server.
	tablets, err := shard.ts.GetTabletMapForShardByCell(ctx, keyspace, shardName, shard.cells)
	if err != nil {
		shard.logger.Errorf("Error fetching tablets for keyspace/shardName %v/%v: %v", keyspace, shardName, err)
		return nil, err
	}
	return parseTabletInfos(tablets), nil
}

// refreshPrimaryShard fetch primary alias from global topo.
func (shard *ConsensusShard) refreshPrimaryShard(ctx context.Context) (string, error) {
	keyspace, shardName := shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard
	si, err := shard.ts.GetShard(ctx, keyspace, shardName)
	if err != nil {
		shard.logger.Errorf("Error calling GetShard: %v", err)
		return "", err
	}
	return topoproto.TabletAliasString(si.PrimaryAlias), nil
}

// findPrimaryFromLocalCell iterates through the instances stored in consensusShard and returns
// the one that's marked as primary
func (shard *ConsensusShard) findPrimaryFromLocalCell() string {
	var latestPrimaryTimestamp time.Time
	var primaryInstance *consensusInstance
	for _, instance := range shard.instances {
		if instance.tablet.Type == topodatapb.TabletType_PRIMARY {
			// It is possible that there are more than one primary in topo server
			// we should compare timestamp to pick the latest one
			// Primary in topo server is updated serially by vtconsensus based on role and term of wesql-server.
			// If all tablet clocks are consistent, the primaryTimestamp of the primary should be monotonically increasing.
			if primaryInstance != nil {
				// If the primaryTimestamp is the same, it means that the tablet clocks may be not consistent.
				// VTConsensus will select the first instance found as the primary.
				// At the same time, an error log will be printed.
				if latestPrimaryTimestamp.Equal(instance.primaryTimeStamp) {
					shard.logger.Errorf("multiple primary found with same timestamp, previous primary alias:%v primarytimestamp:%v, current primary alias:%v primarytimestamp:%v",
						primaryInstance.alias, latestPrimaryTimestamp, instance.alias, instance.primaryTimeStamp)
				} else {
					shard.logger.Warningf("multiple primary found, previous primary alias:%v primarytimestamp:%v, current primary alias:%v primarytimestamp:%v",
						primaryInstance.alias, latestPrimaryTimestamp, instance.alias, instance.primaryTimeStamp)
				}
			}
			if latestPrimaryTimestamp.Before(instance.primaryTimeStamp) {
				if primaryInstance != nil {
					shard.logger.Infof("newer primary found, current primary alias:%v primarytimestamp:%v",
						instance.alias, instance.primaryTimeStamp)
				}
				latestPrimaryTimestamp = instance.primaryTimeStamp
				primaryInstance = instance
			}
		}
	}
	if primaryInstance != nil {
		return primaryInstance.alias
	}
	return ""
}

// parseTabletInfos replaces the replica reports for the shard key
// Note: this is not thread-safe
func parseTabletInfos(tablets map[string]*topo.TabletInfo) []*consensusInstance {
	// collect all replica instances
	var newReplicas []*consensusInstance
	for alias, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		// Only monitor primary, replica and rdonly tablet types
		// for wesql-server, leader is primary, follower is replica, learner is rdonly.
		// TODO: wesql-server consensus logger will be supported in the future. Maybe a new tablet type (eg. TabletType_LOGGER )
		// 		will be added. Bug the LOGGER tablet type logically should be not invisible to vtgate.
		switch tablet.Type {
		case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
			// mysql hostname and port might be empty here if tablet is not running
			// we will treat them as unreachable
			// tablet.MysqlHostname and tablet.MysqlPort are initialized by vttablet running options.
			// tablet.MysqlHostname and tablet.MysqlPort are used for connecting to mysql across pod.
			instanceKey := inst.InstanceKey{
				Hostname: tablet.MysqlHostname,
				Port:     int(tablet.MysqlPort),
			}
			consensusInstance := consensusInstance{
				instanceKey:      &instanceKey,
				tablet:           tablet,
				primaryTimeStamp: logutil.ProtoToTime(tablet.PrimaryTermStartTime),
				alias:            alias,
			}
			newReplicas = append(newReplicas, &consensusInstance)
		}
	}
	sort.Slice(newReplicas[:], func(i, j int) bool {
		return newReplicas[i].alias < newReplicas[j].alias
	})
	return newReplicas
}

// LockShard locks the keyspace-shard on topo server to prevent others from executing conflicting actions.
func (shard *ConsensusShard) LockShard(ctx context.Context, action string) (context.Context, error) {
	if shard.KeyspaceShard.Keyspace == "" || shard.KeyspaceShard.Shard == "" {
		return nil, fmt.Errorf("try to grab lock with incomplete information: %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
	}
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	if shard.unlock != nil {
		return nil, fmt.Errorf("try to grab lock for %s/%s while the shard holds an unlock function", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
	}
	start := time.Now()
	ctx, unlock, err := shard.ts.LockShard(ctx, shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, fmt.Sprintf("VTConsensus repairing %s", action))
	lockShardTimingsMs.Record([]string{action, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return nil, err
	}
	shard.unlock = unlock
	return ctx, nil
}

// UnlockShard unlocks the keyspace-shard on topo server
// and set the unlock function to nil in the container
func (shard *ConsensusShard) UnlockShard() {
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	if shard.unlock == nil {
		shard.logger.Warningf("Shard %s/%s does not hold a lock", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
		return
	}
	var err error
	shard.unlock(&err)
	shard.unlock = nil
}

// findTabletByHostAndPort find tablet instance with the given tablet hostname
// and port
func (shard *ConsensusShard) findTabletByHostAndPort(host string, port int) *consensusInstance {
	for _, instance := range shard.instances {
		if instance.instanceKey.Hostname == host && instance.instanceKey.Port == port {
			return instance
		}
	}
	return nil
}

// populateVTConsensusStatusLocked populate consensusShard status with a lock
func (shard *ConsensusShard) populateVTConsensusStatusLocked() {
	var instanceList []string
	for _, instance := range shard.instances {
		instanceList = append(instanceList, instance.alias)
	}
	shard.shardStatusCollector.status.Instances = instanceList
	if primary := shard.findShardPrimaryTablet(); primary != nil {
		shard.shardStatusCollector.status.Primary = primary.alias
	}
}

// GetUnlock returns the unlock function for the shard for testing
func (shard *ConsensusShard) GetUnlock() func(*error) {
	shard.unlockMu.Lock()
	defer shard.unlockMu.Unlock()
	return shard.unlock
}
