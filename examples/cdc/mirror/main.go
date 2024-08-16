package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/cdc"
	"github.com/wesql/sqlparser/go/sqltypes"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	vtgatepb "github.com/wesql/sqlparser/go/vt/proto/vtgate"
	"google.golang.org/protobuf/encoding/prototext"
	"log"
)

/*
*
* Mock data:
* Before running the program, please create the following table and insert some data into it.
create table t1 (c1 int primary key auto_increment, c2 text);

* Once the program is running, you can run the following SQLs to test the program.
insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
insert into t1 (c2) values ('I want you to act as an interviewer.');
insert into t1 (c2) values ('I want you to act as an engineer.');
delete from t1 where c2 = 'I want you to act as an English translator, spelling corrector and improver.';
update t1 set c1 = 12345 where c2 = 'I want you to act as an interviewer.';

* You can compare the data in the source table and the target table to see if the program works as expected.
*/
func main() {
	mockConfig()

	cc := cdc.NewCdcConsumer()
	cc.Open()
	defer cc.Close()

	cc.Run()
}

var targetMetaTableName string

func init() {
	flag.StringVar(&targetMetaTableName, "target_meta_table_name", "t2_meta", "target meta table name")
	cdc.SpiOpen = Open
	cdc.SpiLoadGTIDAndLastPK = loadGTIDAndLastPK
	cdc.SpiStoreGtidAndLastPK = storeGtidAndLastPK
	cdc.SpiStoreTableData = storeTableData
	cdc.SpiClose = Close
}

func Open(cc *cdc.CdcConsumer) {
	query := fmt.Sprintf("create table if not exists %s.%s (id bigint primary key auto_increment, last_gtid varchar(255), last_pk blob, lastpk_str varchar(255))", cdc.DefaultConfig.TableSchema, targetMetaTableName)
	cc.VtgateClient.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: query}})
}

func Close(cc *cdc.CdcConsumer) {
}

func loadGTIDAndLastPK(cc *cdc.CdcConsumer) (string, *querypb.QueryResult, error) {
	// todo cdc: we should use cdc_consumer to store gtid and lastpk
	sql := fmt.Sprintf("select last_gtid, last_pk from %s.%s order by id desc limit 1", cdc.DefaultConfig.TableSchema, targetMetaTableName)
	r, err := cc.VtgateClient.Execute(cc.Ctx, &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: sql}})
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

func storeGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, cc *cdc.CdcConsumer) error {
	if currentGTID == "" && currentPK == nil {
		return nil
	}
	template := fmt.Sprintf("insert into `%s`.`%s` (last_gtid,last_pk,lastpk_str) values (%s,%s,%s)", cdc.DefaultConfig.TableSchema, targetMetaTableName, "%a", "%a", "%a")
	bytes, err := prototext.Marshal(currentPK)
	if err != nil {
		return nil
	}
	recordMetaSQL, err := sqlparser.ParseAndBind(template, sqltypes.StringBindVariable(currentGTID), sqltypes.BytesBindVariable(bytes), sqltypes.StringBindVariable(fmt.Sprintf("%v", currentPK)))
	if err != nil {
		return err
	}
	cc.VtgateClient.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: recordMetaSQL}})
	log.Printf("record gtid and pk: %v", recordMetaSQL)
	return nil
}

func storeTableData(resultList []*cdc.RowResult, cc *cdc.CdcConsumer) error {
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
			sql, err = cdc.GenerateDeleteSQL(rowResult, cc.PkFields, cc.ColumnInfoMap)
			if err != nil {
				return fmt.Errorf("failed to generate delete query: %v", err)
			}

		case cdc.UPDATE:
			sql, err = cdc.GenerateUpdateSQL(rowResult, cc.PkFields, cc.ColumnInfoMap)
			if err != nil {
				return fmt.Errorf("failed to generate update query: %v", err)
			}
		}
		queryList = append(queryList, &querypb.BoundQuery{
			Sql: sql,
		})
	}
	cc.VtgateClient.ExecuteBatch(context.Background(), &vtgatepb.ExecuteBatchRequest{Queries: queryList})
	return nil
}

func mockConfig() {
	cdc.DefaultConfig.TableSchema = "d1"
	cdc.DefaultConfig.SourceTableName = "t1"
	cdc.DefaultConfig.TargetTableName = "t2"
	cdc.DefaultConfig.FilterStatement = "select * from t1"
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"

	targetMetaTableName = "t2_meta"
}
