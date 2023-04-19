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
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	repairTimingsMs    = stats.NewMultiTimings("repairTimingsMs", "time vtconsensus takes to repair", []string{"status", "success"})
	unexpectedLockLost = stats.NewCountersWithMultiLabels("unexpectedLockLost", "unexpected lost of the lock", []string{"Keyspace", "Shard"})

	abortRebootstrap bool
)

func init() {
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.BoolVar(&abortRebootstrap, "abort_rebootstrap", false, "Don't allow vtconsensus to rebootstrap an existing group.")
	})
}

// RepairResultCode is the code for repair
type RepairResultCode string

const (
	// Success means successfully repaired
	Success RepairResultCode = "Success"
	// Fail means failed to repair
	Fail RepairResultCode = "Fail"
	// Noop means do nothing
	Noop RepairResultCode = "Noop"
)

// Repair tries to fix shard based on the diagnose type
func (shard *ConsensusShard) Repair(ctx context.Context, status DiagnoseType) (RepairResultCode, error) {
	shard.Lock()
	defer shard.Unlock()
	var err error
	code := Noop
	switch status {
	case DiagnoseTypeWrongPrimaryTablet:
		shard.logger.Infof("primary tablet and leader mistmatch, vtconsensus repairing...")
		code, err = shard.repairWrongPrimaryTablet(ctx)
	case DiagnoseTypeUnreachablePrimary:
		err = fmt.Errorf("%v，please check vitess primary tablet status manually", errUnreachablePrimaryTablet)
		code = Fail
	case DiagnoseTypeMissingConsensusLeader:
		err = fmt.Errorf("%v，please check wesql-server status manually", errMissingConsensusLeader)
		code = Fail
	case DiagnoseTypeUnreachableConsensusLeader:
		err = fmt.Errorf("%v，please check vitess primary tablet status", errUnreachableLeaderMySQL)
		code = Fail
	case DiagnoseTypeHealthy:
		err = nil
		code = Success
	default:
		err = fmt.Errorf("wesql-server instance and vitess tablet unhealthy, please manually repair")
	}

	return code, err
}

func (shard *ConsensusShard) repairWrongPrimaryTablet(ctx context.Context) (RepairResultCode, error) {
	ctx, err := shard.LockShard(ctx, "repairWrongPrimaryTablet")
	if err != nil {
		shard.logger.Warningf("repairWrongPrimaryTablet fails to grab lock for the shard %v: %v", shard.KeyspaceShard, err)
		return Noop, err
	}
	defer shard.UnlockShard()
	start := time.Now()
	err = shard.fixPrimaryTabletLocked(ctx)
	repairTimingsMs.Record([]string{DiagnoseTypeWrongPrimaryTablet, strconv.FormatBool(err == nil)}, start)
	if err != nil {
		return Fail, err
	}
	return Success, nil
}

// fixPrimaryTabletLocked changes Vitess primary tablet based on mysql consensus global view.
func (shard *ConsensusShard) fixPrimaryTabletLocked(ctx context.Context) error {
	host, port, isOnline := shard.sqlConsensusView.GetPrimary()
	if !isOnline {
		return errUnreachableLeaderMySQL
	}
	// Primary tablet does not run wesql-server leader,
	// we need to change it based on wesql-server leader info accordingly.
	candidate := shard.findTabletByHostAndPort(host, port)
	if candidate == nil {
		return errMissingPrimaryTablet
	}
	// Make sure we still hold the topo server lock before moving on
	if err := shard.checkShardLocked(ctx); err != nil {
		return err
	}
	err := shard.tmc.ChangeType(ctx, candidate.tablet, topodatapb.TabletType_PRIMARY, false)
	if err != nil {
		return fmt.Errorf("failed to change type to primary on %v: %v", candidate.alias, err)
	}
	shard.logger.Infof("Successfully make %v the primary tablet", candidate.alias)
	return nil
}

func (shard *ConsensusShard) checkShardLocked(ctx context.Context) error {
	if err := topo.CheckShardLocked(ctx, shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard); err != nil {
		labels := []string{shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard}
		unexpectedLockLost.Add(labels, 1)
		shard.logger.Errorf("lost topology lock; aborting")
		return vterrors.Wrap(err, "lost topology lock; aborting")
	}
	return nil
}
