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
var sourceTableName string
var targetTableName string
var filterStatement string
var gtid string
var wescaleURL string

func test() {
	tableSchema = "d1"
	sourceTableName = "t1"
	targetTableName = "t2"
	filterStatement = "select * from t1"
	gtid = ""
	wescaleURL = "127.0.0.1:15991"
}

type RowEventType string

const (
	INSERT RowEventType = "insert"
	DELETE RowEventType = "delete"
	UPDATE RowEventType = "update"
)

type RowResult struct {
	RowType RowEventType
	Before  *sqltypes.Result
	After   *sqltypes.Result
}

// create table t1 (c1 int primary key auto_increment, c2 text);
// create table t2 (c1 int primary key auto_increment, c2 text);
// insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
// insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
// insert into t1 (c2) values ('I want you to act as an interviewer.');
// insert into t1 (c2) values ('I want you to act as an engineer.');

// delete from t1 where c2 = 'I want you to act as an English translator, spelling corrector and improver.';

// update t1 set c1 = 12345 where c2 = 'I want you to act as an interviewer.';
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

	// 2. Build ColumnInfo Map
	// todo cdc: consider ddl during execution, then colInfoMap and pkColNames is out of date.
	colInfoMap, err := buildColInfoMap(tableSchema, sourceTableName, func(sql string) (*sqltypes.Result, error) {
		resp, err := client.Execute(context.Background(), &vtgatepb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: sql}})
		if err != nil {
			return nil, err
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("failed to execute query: %v", resp.Error)
		}
		return sqltypes.Proto3ToResult(resp.Result), nil
	})
	pkColNames := getPkColumnsOrderBySeqInIndex(colInfoMap)

	// 3. Create a VStream request.
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: tableSchema,
			Shard:    "0",
			Gtid:     gtid,
			// todo cdc: add lastpk, see example at go/vt/vttablet/tabletserver/vstreamer/rowstreamer.go:237
			//TablePKs: []*binlogdatapb.TableLastPK{{
			//	TableName: sourceTableName,
			//	Lastpk:    sqltypes.ResultToProto3(sqltypes.MakeTestResult(resp.Result.Fields, "1")),
			//}},
		}}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  sourceTableName,
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
	var pkFields []*querypb.Field
	var currentGTID string
	var currentPK *querypb.QueryResult
	var resultList []*RowResult
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
				pkFields = getPkFields(pkColNames, fields)
				fmt.Printf("%v\n", event)
			case binlogdatapb.VEventType_ROW:
				// todo cdc: process update & delete
				for _, rowChange := range event.RowEvent.RowChanges {
					before := false
					after := false
					if rowChange.Before != nil {
						before = true
					}
					if rowChange.After != nil {
						after = true
					}
					switch {
					case !before && after:
						// insert
						res := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
							Fields: fields,
							Rows: []*querypb.Row{
								rowChange.After,
							},
						})
						resultList = append(resultList, &RowResult{RowType: INSERT, Before: nil, After: res})

					case before && !after:
						// delete
						res := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
							Fields: fields,
							Rows: []*querypb.Row{
								rowChange.Before,
							},
						})
						resultList = append(resultList, &RowResult{RowType: DELETE, Before: res, After: nil})

					case before && after:
						// update
						res1 := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
							Fields: fields,
							Rows: []*querypb.Row{
								rowChange.Before,
							},
						})
						res2 := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
							Fields: fields,
							Rows: []*querypb.Row{
								rowChange.After,
							},
						})
						resultList = append(resultList, &RowResult{RowType: UPDATE, Before: res1, After: res2})

					default:
						panic("unreachable code")
					}

				}
			case binlogdatapb.VEventType_VGTID:
				fmt.Println(event)
				if len(event.Vgtid.GetShardGtids()) > 0 && event.Vgtid.GetShardGtids()[0].Gtid != "" {
					currentGTID = event.Vgtid.GetShardGtids()[0].Gtid
					fmt.Println("currentGTID: ", currentGTID)
				}
				if len(event.Vgtid.GetShardGtids()) > 0 && len(event.Vgtid.GetShardGtids()[0].TablePKs) > 0 {
					currentPK = event.Vgtid.GetShardGtids()[0].TablePKs[0].Lastpk
					fmt.Println("currentPK: ", currentPK)
					fullCurrentPK := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
						Fields: pkFields,
						Rows: []*querypb.Row{
							currentPK.Rows[0],
						},
					})
					buf := sqlparser.NewTrackedBuffer(nil)
					generatePKConstraint(buf, fullCurrentPK, colInfoMap)
					fmt.Println(buf)
				}
			case binlogdatapb.VEventType_COMMIT:
				// todo cdc: record pk & gtid with data in the same transaction for crash recovery
				//put data
				//put pk

				if len(resultList) == 0 {
					continue
				}
				insertQueryList := make([]*querypb.BoundQuery, 0)
				for _, rowResult := range resultList {
					var sql string
					var err error
					switch rowResult.RowType {
					case INSERT:
						sql, err = generateInsertSQL(rowResult)
						if err != nil {
							log.Fatalf("failed to generate insert query: %v", err)
						}

					case DELETE:
						sql, err = generateDeleteSQL(rowResult, pkFields)
						if err != nil {
							log.Fatalf("failed to generate delete query: %v", err)
						}

					case UPDATE:
						sql, err = generateUpdateSQL(rowResult, pkFields)
						if err != nil {
							log.Fatalf("failed to generate update query: %v", err)
						}
					}
					insertQueryList = append(insertQueryList, &querypb.BoundQuery{
						Sql: sql,
					})
				}

				r, err := client.ExecuteBatch(context.Background(), &vtgatepb.ExecuteBatchRequest{Queries: insertQueryList})
				if err != nil {
					log.Fatalf("failed to execute batch: %v", err)
				}
				for i, result := range r.Results {
					if result.Error != nil {
						log.Printf("failed to execute query %d: %v", i, result.Error)
					}
				}
				// clear the result list
				resultList = make([]*RowResult, 0)

			case binlogdatapb.VEventType_COPY_COMPLETED:
				fmt.Printf("%v\n", event)
			}
		}
	}
}

func init() {
	pflag.StringVar(&tableSchema, "TABLE_SCHEMA", "", "The table schema.")
	pflag.StringVar(&sourceTableName, "SOURCE_TABLE_NAME", "", "The source table name.")
	pflag.StringVar(&targetTableName, "TARGET_TABLE_NAME", "", "The target table name.")
	pflag.StringVar(&filterStatement, "FILTER_STATEMENT", "", "The filter statement.")
	pflag.StringVar(&gtid, "GTID", "", "The GTID.")
	pflag.StringVar(&wescaleURL, "WESCALE_URL", "", "The WeScale URL.")
}

func checkFlags() error {
	if tableSchema == "" {
		return fmt.Errorf("table-schema is required")
	}
	if sourceTableName == "" {
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
	fieldNameList := make([]string, 0)
	vals := make([]string, 0)
	vars := make([]any, 0)
	for _, field := range result.Fields {
		fieldNameList = append(fieldNameList, field.Name)
		vars = append(vars, sqlparser.String(sqlparser.NewArgument(field.Name)))
		vals = append(vals, "%a")
	}
	queryTemplate := fmt.Sprintf("insert into %s.%s (%s) values (%s)", tableSchema, tableName, strings.Join(fieldNameList, ","), strings.Join(vals, ","))
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

func getCharsetAndCollation(columnName string, colInfoMap map[string]*ColumnInfo) (string, string) {
	if colInfo, ok := colInfoMap[columnName]; ok {
		return colInfo.CharSet, colInfo.Collation
	}
	return "", ""
}

func generatePKConstraint(buf *sqlparser.TrackedBuffer, lastpk *sqltypes.Result, colInfoMap map[string]*ColumnInfo) {
	type charSetCollation struct {
		charSet   string
		collation string
	}
	var charSetCollations []*charSetCollation
	separator := "("
	for _, pkname := range lastpk.Fields {
		charSet, collation := getCharsetAndCollation(pkname.Name, colInfoMap)
		charSetCollations = append(charSetCollations, &charSetCollation{charSet: charSet, collation: collation})
		buf.Myprintf("%s%s%v%s", separator, charSet, &sqlparser.ColName{Name: sqlparser.NewIdentifierCI(pkname.Name)}, collation)
		separator = ","
	}
	separator = ") <= ("
	for i, val := range lastpk.Rows[0] {
		buf.WriteString(separator)
		buf.WriteString(charSetCollations[i].charSet)
		separator = ","
		val.EncodeSQL(buf)
		buf.WriteString(charSetCollations[i].collation)
	}
	buf.WriteString(")")
}

func generateInsertSQL(rowResult *RowResult) (string, error) {
	parsedInsert := generateInsertParsedQuery(tableSchema, targetTableName, rowResult.After)
	bindVars := generateInsertQueryBindVariables(rowResult.After)
	insertSql, err := parsedInsert.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	return insertSql, nil
}

func generateDeleteParsedQuery(tableSchema, tableName string, pkFields []*querypb.Field) *sqlparser.ParsedQuery {
	queryTemplate := fmt.Sprintf("delete from %s.%s", tableSchema, tableName)
	vars := make([]any, 0)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.WriteString(" where ")
	separator := ""
	for _, col := range pkFields {
		buf.Myprintf("%s%s=", separator, col.Name)
		buf.Myprintf("%s", "%a")
		separator = " and "
		vars = append(vars, sqlparser.String(sqlparser.NewArgument(col.Name)))
	}

	queryTemplate = fmt.Sprintf("%s%s", queryTemplate, buf.String())
	return sqlparser.BuildParsedQuery(queryTemplate, vars...)
}

func generateDeleteQueryBindVariables(result *sqltypes.Result, pkFields []*querypb.Field) map[string]*querypb.BindVariable {
	pkMap := make(map[string]bool)
	for _, pkField := range pkFields {
		pkMap[pkField.Name] = true
	}

	bindVars := make(map[string]*querypb.BindVariable)
	for _, namedValues := range result.Named().Rows {
		for colName, value := range namedValues {
			if _, ok := pkMap[colName]; ok {
				bindVars[colName] = sqltypes.ValueBindVariable(value)
			}
		}
	}
	return bindVars
}

func generateDeleteSQL(rowResult *RowResult, pkFields []*querypb.Field) (string, error) {
	parsedDelete := generateDeleteParsedQuery(tableSchema, targetTableName, pkFields)
	bindVars := generateDeleteQueryBindVariables(rowResult.Before, pkFields)
	deleteSQL, err := parsedDelete.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	return deleteSQL, nil
}

func generateUpdateParsedQuery(tableSchema, tableName string, allFields []*querypb.Field, pkFields []*querypb.Field) *sqlparser.ParsedQuery {
	queryTemplate := fmt.Sprintf("update %s.%s", tableSchema, tableName)
	vars := make([]any, 0)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.WriteString(" set ")
	separator := ""
	for _, col := range allFields {
		buf.Myprintf("%s%s=", separator, col.Name)
		buf.Myprintf("%s", "%a")
		separator = ","
		vars = append(vars, sqlparser.String(sqlparser.NewArgument(col.Name)))
	}

	separator = ""
	buf.WriteString(" where ")
	for _, col := range pkFields {
		buf.Myprintf("%s%s=", separator, col.Name)
		buf.Myprintf("%s", "%a")
		separator = " and "
		vars = append(vars, sqlparser.String(sqlparser.NewArgument("pk_"+col.Name)))
	}

	queryTemplate = fmt.Sprintf("%s%s", queryTemplate, buf.String())
	return sqlparser.BuildParsedQuery(queryTemplate, vars...)
}

func generateUpdateQueryBindVariables(before *sqltypes.Result, after *sqltypes.Result, pkFields []*querypb.Field) map[string]*querypb.BindVariable {
	pkMap := make(map[string]bool)
	for _, pkField := range pkFields {
		pkMap[pkField.Name] = true
	}

	// bind vars of set value part
	bindVars := make(map[string]*querypb.BindVariable)
	for _, namedValues := range after.Named().Rows {
		for colName, value := range namedValues {
			bindVars[colName] = sqltypes.ValueBindVariable(value)
		}
	}

	// bind vars of where part
	for _, namedValues := range before.Named().Rows {
		for colName, value := range namedValues {
			if _, ok := pkMap[colName]; ok {
				bindVars["pk_"+colName] = sqltypes.ValueBindVariable(value)
			}
		}
	}
	return bindVars
}

func generateUpdateSQL(rowResult *RowResult, pkFields []*querypb.Field) (string, error) {
	parsedUpdate := generateUpdateParsedQuery(tableSchema, targetTableName, rowResult.Before.Fields, pkFields)
	bindVars := generateUpdateQueryBindVariables(rowResult.Before, rowResult.After, pkFields)
	updateSQL, err := parsedUpdate.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	return updateSQL, nil
}
