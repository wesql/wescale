package topotools

import (
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var defaultKeyspace = "apecloud"
var defaultShardName = "0"

func CreateDatabase(ctx context.Context, ts *topo.Server, gw queryservice.QueryService, keyspaceName string, cells []string) error {
	_, err := ts.GetOrCreateShard(ctx, keyspaceName, defaultShardName)
	if err != nil {
		return fmt.Errorf("CreateKeyspace(%v:%v) failed: %v", keyspaceName, defaultShardName, err)
	}

	// Create Database if not exist
	dbname := keyspaceName
	target := &querypb.Target{Keyspace: defaultKeyspace, Shard: defaultShardName, TabletType: topodatapb.TabletType_PRIMARY}
	sql := "CREATE DATABASE IF NOT EXISTS `" + dbname + "`"
	_, err = gw.Execute(ctx, target, sql, nil, 0, 0, nil)
	if err != nil {
		return fmt.Errorf("error ensuring database exists: %v", err)
	}

	if err = ts.RebuildSrvVSchema(ctx, cells); err != nil {
		return err
	}
	if err = RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, keyspaceName, cells, false); err != nil {
		return err
	}

	return nil
}

func DropDatabase(ctx context.Context, ts *topo.Server, keyspaceName string, cells []string) error {
	if err := ts.DeleteShard(ctx, keyspaceName, defaultShardName); err != nil {
		return err
	}

	for _, cell := range cells {
		if err := ts.DeleteSrvKeyspace(ctx, cell, keyspaceName); err != nil && !topo.IsErrType(err, topo.NoNode) {
			return fmt.Errorf("Cannot delete SrvKeyspace in cell %v for %v: %v", cell, keyspaceName, err)
		}
	}

	if err := ts.DeleteKeyspace(ctx, keyspaceName); err != nil {
		return err
	}

	if err := ts.RebuildSrvVSchema(ctx, cells); err != nil {
		return err
	}

	return nil
}
