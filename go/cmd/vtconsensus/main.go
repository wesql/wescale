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

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus"
)

func main() {
	var clustersToWatch []string
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		// vtconsensus --clusters_to_watch="commerce/-0". Only one ks/shard currently.
		fs.StringSliceVar(&clustersToWatch, "clusters_to_watch", nil, `Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: "ks1,ks2/-80"`)

		acl.RegisterFlags(fs)
	})
	servenv.ParseFlags("vtconsensus")

	// openTabletDiscovery will open up a connection to topo server
	// and populate the tablets in memory,
	// include mysql instance hostname and port in every tablet.
	vtconsensus := vtconsensus.OpenTabletDiscovery(context.Background(), []string{"zone1"}, clustersToWatch)
	// get the latest tablets from topo server
	vtconsensus.RefreshCluster()
	// starts the scanAndFix tablet status.
	vtconsensus.ScanAndRepair()

	// block here so that we don't exit directly
	select {}
}
