package mirror

import (
	"context"
	"fmt"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/go/sqltypes"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	vtgatepb "github.com/wesql/sqlparser/go/vt/proto/vtgate"
	cdc "github.com/wesql/wescale-cdc"
	"google.golang.org/protobuf/encoding/prototext"
	"os"
)

var TargetMetaTableName string

func init() {
	cdc.RegisterStringVar(&TargetMetaTableName, "TARGET_META_TABLE_NAME", "t2_meta", "target meta table name")
	cdc.SpiOpen = Open
	cdc.SpiLoadGTIDAndLastPK = LoadGTIDAndLastPK
	cdc.SpiStoreGtidAndLastPK = StoreGtidAndLastPK
	cdc.SpiStoreTableData = StoreTableData
	cdc.SpiClose = Close
	cdc.SpiInfof = func(format string, args ...any) {
		fmt.Printf(format, args...)
	}
	cdc.SpiFatalf = func(format string, args ...any) {
		fmt.Printf(format, args...)
		os.Exit(1)
	}
}

func Open(cc *cdc.CdcConsumer) {
	createTargetTableQuery := fmt.Sprintf("create table if not exists %s.%s like %s.%s", cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.TargetTableName, cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.SourceTableName)
	resp, err := cc.VtgateClient.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: createTargetTableQuery}})
	if err != nil {
		cdc.SpiInfof("cdc consumer failed to create target table: %v\n", err)
	}
	if resp != nil && resp.Error != nil {
		cdc.SpiInfof("cdc consumer failed to create target table: %v\n", resp.Error)
	}

	createMetaTableQuery := fmt.Sprintf("create table if not exists %s.%s (id bigint primary key auto_increment, last_gtid varchar(255), last_pk blob, lastpk_str varchar(255))", cdc.DefaultConfig.TableSchema, TargetMetaTableName)
	resp, err = cc.VtgateClient.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: createMetaTableQuery}})
	if err != nil {
		cdc.SpiInfof("cdc consumer failed to create meta table: %v\n", err)
	}
	if resp != nil && resp.Error != nil {
		cdc.SpiInfof("cdc consumer failed to create meta table: %v\n", resp.Error)
	}
}

func Close(cc *cdc.CdcConsumer) {
}

func LoadGTIDAndLastPK(cc *cdc.CdcConsumer) (string, *querypb.QueryResult, error) {
	sql := fmt.Sprintf("select last_gtid, last_pk from %s.%s order by id desc limit 1", cdc.DefaultConfig.TableSchema, TargetMetaTableName)
	r, err := cc.VtgateClient.Execute(cc.Ctx, &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: sql}})
	if err != nil {
		return "", nil, err
	}
	if r.Error != nil {
		return "", nil, fmt.Errorf("failed to load gtid and lastpk: %v", r.Error)
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

func StoreGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, cc *cdc.CdcConsumer) error {
	if currentGTID == "" && currentPK == nil {
		return nil
	}
	template := fmt.Sprintf("insert into `%s`.`%s` (last_gtid,last_pk,lastpk_str) values (%s,%s,%s)", cdc.DefaultConfig.TableSchema, TargetMetaTableName, "%a", "%a", "%a")
	bytes, err := prototext.Marshal(currentPK)
	if err != nil {
		return nil
	}
	recordMetaSQL, err := sqlparser.ParseAndBind(template, sqltypes.StringBindVariable(currentGTID), sqltypes.BytesBindVariable(bytes), sqltypes.StringBindVariable(fmt.Sprintf("%v", currentPK)))
	if err != nil {
		return err
	}
	cc.VtgateClient.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: recordMetaSQL}})
	cdc.SpiInfof("cdc consumer record gtid and pk: %v\n", recordMetaSQL)
	return nil
}

func StoreTableData(resultList []*cdc.RowResult, cc *cdc.CdcConsumer) error {
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
