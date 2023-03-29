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

package vtconsensus

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtconsensus/controller"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	refreshInterval       = 10 * time.Second
	scanInterval          = 3 * time.Second
	scanAndRepairTimeout  = 3 * time.Second
	vtconsensusConfigFile string

	localDbPort int
)

func init() {
	// vtconsensus --refresh_interval=10 --scan_interval=3 --scan_repair_timeout=3 --vtconsensus_config="config.file" --db_port=3306
	// vtconsensus_config is required.
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.DurationVar(&refreshInterval, "refresh_interval", 10*time.Second, "Refresh interval to load tablets.")
		fs.DurationVar(&scanInterval, "scan_interval", 3*time.Second, "Scan interval to diagnose and repair.")
		fs.DurationVar(&scanAndRepairTimeout, "scan_repair_timeout", 3*time.Second, "Time to wait for a Diagnose and repair operation.")
		fs.StringVar(&vtconsensusConfigFile, "vtconsensus_config", "", "Config file for vtconsensus.")
		fs.IntVar(&localDbPort, "db_port", 0, "Local mysql port, set this to enable local fast check.")
	})
}

// VTConsensus is the interface to manage the component to set up ApeCloud MySQL with Vitess.
// The main goal of it is to reconcile ApeCloud MySQL and the Vitess topology.
// Caller should use OpenTabletDiscovery to create the VTConsensus instance.
// ApeCloud MySQL Only one shard.
type VTConsensus struct {
	// Shards are all the shards that a VTConsenus is monitoring.
	// Caller can choose to iterate the shards to scan and repair for more granular control (e.g., stats report)
	// instead of calling ScanAndRepair() directly.
	Shard *controller.ConsensusShard
	topo  controller.ConsensusTopo
	tmc   tmclient.TabletManagerClient
	ctx   context.Context

	stopped sync2.AtomicBool
}

func newVTConsensus(ctx context.Context, ts controller.ConsensusTopo, tmc tmclient.TabletManagerClient) *VTConsensus {
	return &VTConsensus{
		topo: ts,
		tmc:  tmc,
		ctx:  ctx,
	}
}

// OpenTabletDiscovery calls OpenTabletDiscoveryWithAcitve and set the shard to be active
// it opens connection with topo server
// and triggers the first round of controller based on specified cells and keyspace/shards.
func OpenTabletDiscovery(ctx context.Context, cellsToWatch []string, clustersToWatch string) *VTConsensus {
	var shard *controller.ConsensusShard
	// TODO: vtconsensusConfigurator is not used, remove it currently. In the future, we may need it, add it back.

	// new a vtconsensus instance, include topo server and tablet manager client
	vtconsensus := newVTConsensus(
		ctx,
		topo.Open(),
		tmclient.NewTabletManagerClient(),
	)

	// create a context with timeout, RemoteOperationTimeout is default 30s.
	ctx, cancel := context.WithTimeout(vtconsensus.ctx, topo.RemoteOperationTimeout)
	defer cancel()

	// if clustersToWatch contains "/", it is a keyspace/shard specification
	if strings.Contains(clustersToWatch, "/") {
		input := strings.Split(clustersToWatch, "/")
		// input[0] is keyspace, input[1] is shard 0.
		if input[1] != "0" {
			log.Exitf("Shard must be 0 for ks: %v", clustersToWatch)
		}
		shard = controller.NewConsensusShard(
			input[0],
			input[1],
			cellsToWatch,
			vtconsensus.tmc,
			vtconsensus.topo,
			db.NewVTConsensusSQLAgent(),
			localDbPort)
	} else {
		// This is only a keyspace, find shard 0 in keyspace.
		shardNames, err := vtconsensus.topo.GetShardNames(ctx, clustersToWatch)
		if err != nil {
			log.Exitf("Error fetching shards for keyspace %v: %v", clustersToWatch, err)
		}
		if len(shardNames) != 1 || shardNames[0] != "0" {
			log.Exitf("Shard must be 0 for ks: %v", clustersToWatch)
		}
		shard = controller.NewConsensusShard(
			clustersToWatch,
			shardNames[0],
			cellsToWatch,
			vtconsensus.tmc,
			vtconsensus.topo,
			db.NewVTConsensusSQLAgent(),
			localDbPort)
	}

	vtconsensus.handleSignal(os.Exit)
	vtconsensus.Shard = shard
	log.Infof("Monitoring shard %v", vtconsensus.Shard)
	// Force refresh all tablet here to populate data for vtconsensus
	var wg sync.WaitGroup
	wg.Add(1)
	go func(shard *controller.ConsensusShard) {
		defer wg.Done()
		shard.UpdateTabletsInShardWithLock(ctx)
	}(shard)
	wg.Wait()

	log.Info("Ready to start VTConsensus")
	return vtconsensus
}

// RefreshCluster get the latest tablets from topo server and update the ConsensusShard.
func (vtconsensus *VTConsensus) RefreshCluster() {
	// start thread
	go func(shard *controller.ConsensusShard) {
		// period timer task, refresh tablet info in shard.
		log.Infof("Start refresh tablet period task in the %v/%v, refresh_interval is %v seconds",
			shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, refreshInterval.Seconds())
		ticker := time.Tick(refreshInterval)
		for range ticker {
			ctx, cancel := context.WithTimeout(vtconsensus.ctx, refreshInterval)
			shard.UpdateTabletsInShardWithLock(ctx)
			cancel()
		}
	}(vtconsensus.Shard)
}

// ScanAndRepair starts the scanAndFix routine
func (vtconsensus *VTConsensus) ScanAndRepair() {
	go func(shard *controller.ConsensusShard) {
		log.Infof("Start scan and repair tablet in the %v/%v, scan_interval is %v seconds, scan_repair_timeout is %v seconds",
			shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard,
			scanInterval.Seconds(), scanAndRepairTimeout.Seconds())
		ticker := time.Tick(scanInterval)
		for range ticker {
			func() {
				ctx, cancel := context.WithTimeout(vtconsensus.ctx, scanAndRepairTimeout)
				defer cancel()
				if !vtconsensus.stopped.Get() {
					shard.ScanAndRepairShard(ctx)
				}
			}()
		}
	}(vtconsensus.Shard)
}

func (vtconsensus *VTConsensus) handleSignal(action func(int)) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		// block until the signal is received
		<-sigChan
		log.Infof("Handling SIGHUP")
		// Set stopped to true so that following repair call won't do anything
		// For the ongoing repairs, checkShardLocked will abort if needed
		vtconsensus.stopped.Set(true)
		vtconsensus.Shard.UnlockShard()

		action(1)
	}()
}
