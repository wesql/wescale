package main

import (
	"context"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	"github.com/wesql/sqlparser/go/vt/proto/vtgateservice"
)

var SpiOpen func(client vtgateservice.VitessClient)

var SpiLoadGTIDAndLastPK func(ctx context.Context, client vtgateservice.VitessClient) (string, *querypb.QueryResult, error)

var SpiStoreGtidAndLastPK func(currentGTID string, currentPK *querypb.QueryResult, client vtgateservice.VitessClient) error

var SpiStoreTableData func(resultList []*RowResult, colInfoMap map[string]*ColumnInfo, pkFields []*querypb.Field, client vtgateservice.VitessClient) error

var SpiClose func(client vtgateservice.VitessClient)
