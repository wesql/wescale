/*
Copyright ApeCloud, Inc.

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

package main

import (
	"context"

	"vitess.io/vitess/go/vt/log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus"
)

func main() {
	var clustersToWatch string
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		// vtconsensus --clusters_to_watch="commerce/-0" or --clusters_to_watch="commerce".
		// "commerce" is the default keyspace specified by the user,
		// and "shard 0" indicates that this keyspace is an unsharded keyspace
		// meaning that keyspace has only one shard.
		// Currently, wesqlscale only supports unsharded keyspace.
		// If "cluster_to_watch" does not specify a shard 0, then it will directly look for shard 0.
		fs.StringVar(&clustersToWatch, "clusters_to_watch", clustersToWatch, `Keyspace or keyspace/shards that this instance will monitor and repair, If "cluster_to_watch" does not specify a shard, then it will directly look for shard 0. Example: "ks1 or ks1/0"`)

		acl.RegisterFlags(fs)
	})
	servenv.ParseFlags("vtconsensus")

	if len(clustersToWatch) == 0 {
		log.Exitf("clusters_to_watch must be configured")
	}

	// openTabletDiscovery will open up a connection to topo server
	// and populate the tablets in memory,
	// include mysql instance hostname and port in every tablet.
	vtconsensus := vtconsensus.OpenTabletDiscovery(context.Background(), nil, clustersToWatch)
	// get the latest tablets from topo server
	vtconsensus.RefreshCluster()
	// starts the scanAndRepair tablet status.
	vtconsensus.ScanAndRepair()

	// block here so that we don't exit directly
	select {}
}
