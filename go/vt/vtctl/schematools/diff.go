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

package schematools

import (
	"context"
	"fmt"

	"github.com/wesql/wescale/go/vt/mysqlctl/tmutils"
	"github.com/wesql/wescale/go/vt/topo"
	"github.com/wesql/wescale/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "github.com/wesql/wescale/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/wesql/wescale/go/vt/proto/topodata"
)

// CompareSchemas returns (nil, nil) if the schema of the two tablets match. If
// there are diffs, they are returned as (diffs []string, nil).
//
// If fetching the schema for either tablet fails, a non-nil error is returned.
func CompareSchemas(
	ctx context.Context,
	ts *topo.Server,
	tmc tmclient.TabletManagerClient,
	source *topodatapb.TabletAlias,
	dest *topodatapb.TabletAlias,
	sourceKeyspace string,
	destKeyspace string,
	tables []string,
	excludeTables []string,
	includeViews bool,
) (diffs []string, err error) {
	sourceReq := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews, DbName: sourceKeyspace}
	sourceSchema, err := GetSchema(ctx, ts, tmc, source, sourceReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from tablet %v. err: %v", source, err)
	}

	destReq := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews, DbName: destKeyspace}
	destSchema, err := GetSchema(ctx, ts, tmc, dest, destReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from tablet %v. err: %v", dest, err)
	}

	return tmutils.DiffSchemaToArray("source", sourceSchema, "dest", destSchema), nil
}
