/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"context"
	"fmt"
	"github.com/spf13/pflag"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/go/sqltypes"
	binlogdatapb "github.com/wesql/sqlparser/go/vt/proto/binlogdata"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	topodatapb "github.com/wesql/sqlparser/go/vt/proto/topodata"
	vtgatepb "github.com/wesql/sqlparser/go/vt/proto/vtgate"
	"github.com/wesql/sqlparser/go/vt/proto/vtgateservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"strings"
)

var tableSchema string
var tableName string
var filterStatement string
var gtid string
var wescaleURL string

func test() {
	tableSchema = "d1"
	tableName = "t1"
	filterStatement = "select * from t1"
	gtid = ""
	wescaleURL = "127.0.0.1:15991"
}

// create table t1 (c1 int primary key auto_increment, c2 text);
// insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
// insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
// insert into t1 (c2) values ('I want you to act as an interviewer.');
func main() {

	test()

	pflag.Parse()
	err := checkFlags()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	// 1. Connect to the vtgate server.
	client, closeFunc, err := openWeScaleClient()
	if err != nil {
		log.Fatalf("failed to connect to vtgate: %v", err)
	}
	defer closeFunc()

	// 3. Create a VStream request.
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: tableSchema,
			Shard:    "0",
			Gtid:     gtid,
			//TablePKs: []*binlogdatapb.TableLastPK{{
			//	TableName: tableName,
			//	Lastpk:    sqltypes.ResultToProto3(sqltypes.MakeTestResult(resp.Result.Fields, "1")),
			//}},
		}}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  tableName,
			Filter: filterStatement,
		}},
	}
	flags := &vtgatepb.VStreamFlags{}
	req := &vtgatepb.VStreamRequest{
		TabletType: topodatapb.TabletType_PRIMARY,
		Vgtid:      vgtid,
		Filter:     filter,
		Flags:      flags,
	}
	reader, err := client.VStream(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to create vstream: %v", err)
	}
	fmt.Printf("start streaming\n\n\n\n")

	// 4. Read the stream and process the events.
	var fields []*querypb.Field
	var currentGTID string
	var currentPK *querypb.QueryResult
	var resultList []*sqltypes.Result
	for {
		resp, err := reader.Recv()
		if err == io.EOF {
			fmt.Printf("stream ended\n")
			return
		}
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		eventList := resp.Events
		for _, event := range eventList {
			switch event.Type {
			case binlogdatapb.VEventType_FIELD:
				fields = event.FieldEvent.Fields
				fmt.Printf("%v\n", event)
			case binlogdatapb.VEventType_ROW:
				res := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
					Fields: fields,
					Rows: []*querypb.Row{
						event.RowEvent.RowChanges[0].After,
					},
				})
				resultList = append(resultList, res)
			case binlogdatapb.VEventType_VGTID:
				if event.Gtid != "" {
					currentGTID = event.Gtid
				}
				if event.LastPKEvent != nil && event.LastPKEvent.TableLastPK.Lastpk != nil {
					currentPK = event.LastPKEvent.TableLastPK.Lastpk
				}
				fmt.Println("currentGTID: ", currentGTID)
				fmt.Println("currentPK: ", currentPK)
			case binlogdatapb.VEventType_COMMIT:
				//put data
				//put pk

				if len(resultList) == 0 {
					continue
				}
				insertQueryList := make([]*querypb.BoundQuery, 0)
				for _, res := range resultList {
					parsedInsert := generateInsertParsedQuery(tableSchema, "t2", res)
					insertSql, err := parsedInsert.GenerateQuery(generateInsertQueryBindVariables(res), nil)
					if err != nil {
						log.Fatalf("failed to generate insert query: %v", err)
					}
					insertQueryList = append(insertQueryList, &querypb.BoundQuery{
						Sql: insertSql,
					})
				}

				r, err := client.ExecuteBatch(context.Background(), &vtgatepb.ExecuteBatchRequest{Queries: insertQueryList})
				if err != nil {
					log.Fatalf("failed to execute batch: %v", err)
				}
				fmt.Printf("inserted %d rows\n", r.Results[0].Result.RowsAffected)
				// clear the result list
				resultList = make([]*sqltypes.Result, 0)

			case binlogdatapb.VEventType_COPY_COMPLETED:
				fmt.Printf("%v\n", event)
			}
		}
	}
}

func init() {
	pflag.StringVar(&tableSchema, "TABLE_SCHEMA", "", "The table schema.")
	pflag.StringVar(&tableName, "TABLE_NAME", "", "The table name.")
	pflag.StringVar(&filterStatement, "FILTER_STATEMENT", "", "The filter statement.")
	pflag.StringVar(&gtid, "GTID", "", "The GTID.")
	pflag.StringVar(&wescaleURL, "WESCALE_URL", "", "The WeScale URL.")
}

func checkFlags() error {
	if tableSchema == "" {
		return fmt.Errorf("table-schema is required")
	}
	if tableName == "" {
		return fmt.Errorf("table-name is required")
	}
	if filterStatement == "" {
		return fmt.Errorf("filter-statement is required")
	}
	if wescaleURL == "" {
		return fmt.Errorf("we-scale-url is required")
	}
	return nil
}

func openWeScaleClient() (vtgateservice.VitessClient, func(), error) {
	conn, err := grpc.Dial(wescaleURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to vtgate: %v", err)
	}
	client := vtgateservice.NewVitessClient(conn)
	closeFunc := func() {
		conn.Close()
	}
	return client, closeFunc, nil
}

func generateInsertParsedQuery(tableSchema, tableName string, result *sqltypes.Result) *sqlparser.ParsedQuery {
	vals := make([]string, 0)
	vars := make([]any, 0)
	for _, namedValues := range result.Named().Rows {
		for colName := range namedValues {
			vals = append(vals, "%a")
			vars = append(vars, sqlparser.String(sqlparser.NewArgument(colName)))
		}
	}
	queryTemplate := fmt.Sprintf("insert into %s.%s values (%s)", tableSchema, tableName, strings.Join(vals, ","))
	return sqlparser.BuildParsedQuery(queryTemplate, vars...)
}

func generateInsertQueryBindVariables(result *sqltypes.Result) map[string]*querypb.BindVariable {
	bindVars := make(map[string]*querypb.BindVariable)
	for _, namedValues := range result.Named().Rows {
		for colName, value := range namedValues {
			bindVars[colName] = sqltypes.ValueBindVariable(value)
		}
	}
	return bindVars
}
