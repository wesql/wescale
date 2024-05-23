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

package fuzzing

import (
	"context"
	"sync"
	"testing"

	"github.com/wesql/wescale/go/mysql"
	"github.com/wesql/wescale/go/mysql/fakesqldb"
	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/vt/dbconfigs"
	"github.com/wesql/wescale/go/vt/mysqlctl/fakemysqldaemon"
	"github.com/wesql/wescale/go/vt/vttablet/tabletmanager"
	"github.com/wesql/wescale/go/vt/vttablet/tabletservermock"

	tabletmanagerdatapb "github.com/wesql/wescale/go/vt/proto/tabletmanagerdata"
)

var fuzzInitter sync.Once

func initTesting() {
	testing.Init()
}

func FuzzTabletManagerExecuteFetchAsDba(data []byte) int {
	fuzzInitter.Do(initTesting)
	t := &testing.T{}
	ctx := context.Background()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := fakemysqldaemon.NewFakeMysqlDaemon(db)

	dbName := "dbname"
	tm := &tabletmanager.TabletManager{
		MysqlDaemon:         daemon,
		DBConfigs:           dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl: tabletservermock.NewController(),
	}
	_, _ = tm.ExecuteFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   data,
		DbName:  dbName,
		MaxRows: 10,
	})
	return 1
}
