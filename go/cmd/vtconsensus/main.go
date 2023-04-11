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

package main

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/internal/global"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus"
)

func main() {
	var clustersToWatch string
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		// If "cluster_to_watch" does not specify, then it will directly look for default keyspace and shard.
		fs.StringVar(&clustersToWatch, "clusters_to_watch", clustersToWatch, `Keyspace or keyspace/shards that this instance will monitor and repair, default _vt/0 `)

		acl.RegisterFlags(fs)
	})
	servenv.ParseFlags("vtconsensus")

	if len(clustersToWatch) == 0 {
		clustersToWatch = fmt.Sprintf("%s/%s", global.DefaultKeyspace, global.DefaultShard)
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
