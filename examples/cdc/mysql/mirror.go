package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/cdc"
	"github.com/wesql/sqlparser/go/sqltypes"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	vtgatepb "github.com/wesql/sqlparser/go/vt/proto/vtgate"
	"github.com/wesql/sqlparser/go/vt/proto/vtgateservice"
	"google.golang.org/protobuf/encoding/prototext"
	"log"
)

func init() {
	cdc.SpiOpen = Open
	cdc.SpiLoadGTIDAndLastPK = loadGTIDAndLastPK
	cdc.SpiStoreGtidAndLastPK = storeGtidAndLastPK
	cdc.SpiStoreTableData = storeTableData
	cdc.SpiClose = Close
}

func Open(client vtgateservice.VitessClient) {
	query := fmt.Sprintf("create table if not exists %s.%s (id bigint primary key auto_increment, last_gtid varchar(255), last_pk blob, lastpk_str varchar(255))", cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.TargetMetaTableName)
	client.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: query}})
}

func Close(client vtgateservice.VitessClient) {
}

func loadGTIDAndLastPK(ctx context.Context, client vtgateservice.VitessClient) (string, *querypb.QueryResult, error) {
	// todo cdc: we should use cdc_consumer to store gtid and lastpk
	sql := fmt.Sprintf("select last_gtid, last_pk from %s.%s order by id desc limit 1", cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.TargetMetaTableName)
	r, err := client.Execute(ctx, &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: sql}})
	if err != nil || r.Error != nil {
		return "", nil, errors.New("failed to load gtid and lastpk")
	}
	res := sqltypes.CustomProto3ToResult(r.Result.Fields, r.Result)
	if len(res.Rows) == 0 {
		return "", nil, nil
	}
	gtid := res.Named().Rows[0]["last_gtid"].ToString()
	lastPKBytes, err := res.Named().Rows[0]["last_pk"].ToBytes()
	if err != nil {
		return gtid, nil, err
	}
	lastPK := querypb.QueryResult{}
	err = prototext.Unmarshal(lastPKBytes, &lastPK)
	if err != nil {
		return gtid, nil, err
	}
	return gtid, &lastPK, nil
}

func storeGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, client vtgateservice.VitessClient) error {
	if currentGTID == "" && currentPK == nil {
		return nil
	}
	template := fmt.Sprintf("insert into `%s`.`%s` (last_gtid,last_pk,lastpk_str) values (%s,%s,%s)", cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.TargetMetaTableName, "%a", "%a", "%a")
	bytes, err := prototext.Marshal(currentPK)
	if err != nil {
		return nil
	}
	recordMetaSQL, err := sqlparser.ParseAndBind(template, sqltypes.StringBindVariable(currentGTID), sqltypes.BytesBindVariable(bytes), sqltypes.StringBindVariable(fmt.Sprintf("%v", currentPK)))
	if err != nil {
		return err
	}
	client.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: recordMetaSQL}})
	log.Printf("record gtid and pk: %v", recordMetaSQL)
	return nil
}

func storeTableData(resultList []*cdc.RowResult, colInfoMap map[string]*cdc.ColumnInfo, pkFields []*querypb.Field, client vtgateservice.VitessClient) error {
	queryList := make([]*querypb.BoundQuery, 0)
	for _, rowResult := range resultList {
		var sql string
		var err error
		switch rowResult.RowType {
		case cdc.INSERT:
			sql, err = cdc.GenerateInsertSQL(rowResult)
			if err != nil {
				return fmt.Errorf("failed to generate insert query: %v", err)
			}

		case cdc.DELETE:
			sql, err = cdc.GenerateDeleteSQL(rowResult, pkFields, colInfoMap)
			if err != nil {
				return fmt.Errorf("failed to generate delete query: %v", err)
			}

		case cdc.UPDATE:
			sql, err = cdc.GenerateUpdateSQL(rowResult, pkFields, colInfoMap)
			if err != nil {
				return fmt.Errorf("failed to generate update query: %v", err)
			}
		}
		queryList = append(queryList, &querypb.BoundQuery{
			Sql: sql,
		})
	}
	client.ExecuteBatch(context.Background(), &vtgatepb.ExecuteBatchRequest{Queries: queryList})
	return nil
}

func mockConfig() {
	cdc.DefaultConfig.TableSchema = "d1"
	cdc.DefaultConfig.SourceTableName = "t1"
	cdc.DefaultConfig.TargetTableName = "t2"
	cdc.DefaultConfig.TargetMetaTableName = "t2_meta"
	cdc.DefaultConfig.FilterStatement = "select * from t1"
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"
}

func main() {
	mockConfig()

	cc := cdc.NewCdcConsumer()
	cc.Open()
	defer cc.Close()

	cc.Run()
}
