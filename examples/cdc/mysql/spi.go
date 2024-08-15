package main

import (
	"context"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	"github.com/wesql/sqlparser/go/vt/proto/vtgateservice"
)

var SpiOpen func()

var SpiLoadGTIDAndLastPK func(ctx context.Context, client vtgateservice.VitessClient) (string, *querypb.QueryResult, error)

var SpiStoreGtidAndLastPK func(currentGTID string, currentPK *querypb.QueryResult, client vtgateservice.VitessClient, queryList []*querypb.BoundQuery) error

var SpiStoreTableData func(resultList []*RowResult, client vtgateservice.VitessClient, queryList []*querypb.BoundQuery, pkFields []*querypb.Field, colInfoMap map[string]*ColumnInfo) error
