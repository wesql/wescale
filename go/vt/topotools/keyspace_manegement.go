/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package topotools

import (
	"fmt"

	"vitess.io/vitess/go/vt/failpointkey"

	"github.com/pingcap/failpoint"

	"vitess.io/vitess/go/internal/global"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

var defaultKeyspace = global.DefaultKeyspace
var defaultShardName = global.DefaultShard

func CreateDatabase(ctx context.Context, ts *topo.Server, gw queryservice.QueryService, keyspaceName string, cells []string) error {
	return createDatabaseInternal(ctx, ts, func() error {
		// Create Database if not exist
		dbname := keyspaceName
		target := &querypb.Target{Keyspace: defaultKeyspace, Shard: defaultShardName, TabletType: topodatapb.TabletType_PRIMARY}
		sql := "CREATE DATABASE IF NOT EXISTS `" + dbname + "`"
		_, err := gw.Execute(ctx, target, sql, nil, 0, 0, nil)
		return err
	}, keyspaceName, cells)
}

func DropDatabase(ctx context.Context, ts *topo.Server, gw queryservice.QueryService, keyspaceName string, cells []string) error {
	return dropDatabaseInternal(ctx, ts, func() error {
		dbname := keyspaceName
		target := &querypb.Target{Keyspace: defaultKeyspace, Shard: defaultShardName, TabletType: topodatapb.TabletType_PRIMARY}
		sql := "DROP DATABASE IF EXISTS `" + dbname + "`"
		_, err := gw.Execute(ctx, target, sql, nil, 0, 0, nil)
		if err != nil {
			return err
		}
		err = gw.DropSchema(ctx, target, dbname)
		return err
	}, keyspaceName, cells)
}

func CreateDatabaseMeta(ctx context.Context, ts *topo.Server, keyspaceName string, cells []string) error {
	return createDatabaseInternal(ctx, ts, func() error { return nil }, keyspaceName, cells)
}

func DropDatabaseMeta(ctx context.Context, ts *topo.Server, keyspaceName string, cells []string) error {
	return dropDatabaseInternal(ctx, ts, func() error { return nil }, keyspaceName, cells)
}

func createDatabaseInternal(ctx context.Context, ts *topo.Server, f func() error, keyspaceName string, cells []string) error {
	_, err := ts.GetOrCreateShard(ctx, keyspaceName, defaultShardName)
	if err != nil {
		return fmt.Errorf("CreateKeyspace(%v:%v) failed: %v", keyspaceName, defaultShardName, err)
	}

	if v, _err_ := failpoint.Eval(_curpkg_(failpointkey.CreateDatabaseErrorOnDbname.Name)); _err_ == nil {
		if v != nil && v.(string) == keyspaceName {
			return fmt.Errorf("create-database-error-on-dbname error injected")
		}
	}

	err = f()
	if err != nil {
		return err
	}

	if err = ts.RebuildSrvVSchema(ctx, cells); err != nil {
		return err
	}
	if err = RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, keyspaceName, cells, false); err != nil {
		return err
	}

	return nil
}

func dropDatabaseInternal(ctx context.Context, ts *topo.Server, f func() error, keyspaceName string, cells []string) error {
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

	err := f()
	if err != nil {
		return err
	}

	return nil
}
