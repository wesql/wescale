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

// Package testenv supplies test functions for testing vstreamer.
package testenv

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/background"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// Env contains all the env vars for a test against a mysql instance.
type Env struct {
	cluster *vttest.LocalCluster

	KeyspaceName string
	ShardName    string
	Cells        []string

	TabletEnv    tabletenv.Env
	TaskPool     *background.TaskPool
	TopoServ     *topo.Server
	SrvTopo      srvtopo.Server
	Dbcfgs       *dbconfigs.DBConfigs
	Mysqld       *mysqlctl.Mysqld
	SchemaEngine *schema.Engine
	Flavor       string
	// MySQL and Percona are considered equivalent here and both called mysql
	DBType         string
	DBMajorVersion int
	DBMinorVersion int
	DBPatchVersion int
}

// Init initializes an Env.
func Init() (*Env, error) {
	te := &Env{
		KeyspaceName: "vttest",
		ShardName:    "0",
		Cells:        []string{"cell1"},
	}

	ctx := context.Background()
	te.TopoServ = memorytopo.NewServer(te.Cells...)
	if err := te.TopoServ.CreateKeyspace(ctx, te.KeyspaceName, &topodatapb.Keyspace{}); err != nil {
		return nil, err
	}
	if err := te.TopoServ.CreateShard(ctx, te.KeyspaceName, te.ShardName); err != nil {
		panic(err)
	}
	te.SrvTopo = srvtopo.NewResilientServer(te.TopoServ, "TestTopo")

	cfg := vttest.Config{
		Topology: &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: te.KeyspaceName,
					Shards: []*vttestpb.Shard{
						{
							Name:           "0",
							DbNameOverride: "vttest",
						},
					},
				},
			},
		},
		OnlyMySQL: true,
		Charset:   "utf8mb4_general_ci",
	}
	te.cluster = &vttest.LocalCluster{
		Config: cfg,
	}
	if err := te.cluster.Setup(); err != nil {
		os.RemoveAll(te.cluster.Config.SchemaDir)
		return nil, fmt.Errorf("could not launch mysql: %v", err)
	}
	te.Dbcfgs = dbconfigs.NewTestDBConfigs(te.cluster.MySQLConnParams(), te.cluster.MySQLAppDebugConnParams(), te.cluster.DbName())
	config := tabletenv.NewDefaultConfig()
	config.DB = te.Dbcfgs
	te.TabletEnv = tabletenv.NewEnv(config, "VStreamerTest")
	te.TaskPool = background.NewTaskPool(te.TabletEnv)
	te.Mysqld = mysqlctl.NewMysqld(te.Dbcfgs)
	pos, _ := te.Mysqld.PrimaryPosition()
	te.Flavor = pos.GTIDSet.Flavor()
	if strings.HasPrefix(strings.ToLower(te.Flavor), string(mysqlctl.FlavorMariaDB)) {
		te.DBType = string(mysqlctl.FlavorMariaDB)
	} else {
		// MySQL and Percona are equivalent for the tests
		te.DBType = string(mysqlctl.FlavorMySQL)
	}
	dbVersionStr := te.Mysqld.GetVersionString()
	dbVersionStrParts := strings.Split(dbVersionStr, ".")
	var err error
	te.DBMajorVersion, err = strconv.Atoi(dbVersionStrParts[0])
	if err != nil {
		return nil, fmt.Errorf("could not parse database major version from '%s': %v", dbVersionStr, err)
	}
	te.DBMinorVersion, err = strconv.Atoi(dbVersionStrParts[1])
	if err != nil {
		return nil, fmt.Errorf("could not parse database minor version from '%s': %v", dbVersionStr, err)
	}
	te.DBPatchVersion, err = strconv.Atoi(dbVersionStrParts[2])
	if err != nil {
		return nil, fmt.Errorf("could not parse database patch version from '%s': %v", dbVersionStr, err)
	}

	te.SchemaEngine = schema.NewEngine(te.TabletEnv, te.TaskPool)
	te.SchemaEngine.InitDBConfig(te.Dbcfgs.DbaWithDB())

	te.TaskPool.Open()
	if err := te.SchemaEngine.Open(); err != nil {
		return nil, err
	}

	// The first vschema should not be empty. Leads to Node not found error.
	// TODO(sougou): need to fix the bug.
	if err := te.SetVSchema(`{"sharded": true}`); err != nil {
		te.Close()
		return nil, err
	}

	return te, nil
}

// Close tears down TestEnv.
func (te *Env) Close() {
	te.SchemaEngine.Close()
	te.TaskPool.Close()
	te.Mysqld.Close()
	te.cluster.TearDown()
	os.RemoveAll(te.cluster.Config.SchemaDir)
}

// SetVSchema sets the vschema for the test keyspace.
func (te *Env) SetVSchema(vs string) error {
	ctx := context.Background()
	var kspb vschemapb.Keyspace
	if err := json2.Unmarshal([]byte(vs), &kspb); err != nil {
		return err
	}
	if err := te.TopoServ.SaveVSchema(ctx, te.KeyspaceName, &kspb); err != nil {
		return err
	}
	te.SchemaEngine.Reload(ctx)
	return te.TopoServ.RebuildSrvVSchema(ctx, te.Cells)
}

// In MySQL 8.0 and later information_schema no longer contains the display width for integer types and
// as of 8.0.19 for year types as this was an unnecessary headache because it can only serve to confuse
// if the display width is less than the type width (8.0 no longer supports the 2 digit YEAR). So if the
// test is running against MySQL 8.0 or later then you should use this function to replace e.g.
// `int([0-9]*)` with `int` in the expected results string that we define in the test.
func (te *Env) RemoveAnyDeprecatedDisplayWidths(orig string) string {
	if te.DBType != string(mysqlctl.FlavorMySQL) || te.DBMajorVersion < 8 {
		return orig
	}
	var adjusted string
	baseIntType := "int"
	intRE := regexp.MustCompile(`(?i)int\(([0-9]*)?\)`)
	adjusted = intRE.ReplaceAllString(orig, baseIntType)
	if (te.DBMajorVersion > 8 || te.DBMinorVersion > 0) || te.DBPatchVersion >= 19 {
		baseYearType := "year"
		yearRE := regexp.MustCompile(`(?i)year\(([0-9]*)?\)`)
		adjusted = yearRE.ReplaceAllString(adjusted, baseYearType)
	}
	return adjusted
}
