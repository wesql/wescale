/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"context"
	"fmt"
	"github.com/spf13/pflag"
	"github.com/tmc/langchaingo/vectorstores"
	"google.golang.org/grpc"
	"io"
	"log"
	"net/url"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores/qdrant"
)

var tableSchema string
var tableName string
var filterStatement string
var gtid string

var weScaleUrl string

var embeddingModel string
var embeddingUrl string

var vectorStoreUrl string
var vectorStoreCollectionName string

var store vectorstores.VectorStore

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
	// 2. Initialize the embedding service and the vector store.
	initVectorStore()

	// 3. Create a VStream request.
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: tableSchema,
			Shard:    "0",
			Gtid:     gtid,
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

	// 4. Read the stream and process the events.
	var fields []*querypb.Field
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
				fmt.Printf("%v\n", event.FieldEvent.Fields)
				fields = event.FieldEvent.Fields
			case binlogdatapb.VEventType_ROW:
				vals := sqltypes.MakeRowTrusted(fields, event.RowEvent.RowChanges[0].After)
				fmt.Printf("%v\n", event.RowEvent.RowChanges)
				fmt.Printf("%v\n", vals)

				res := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
					Fields: fields,
					Rows: []*querypb.Row{
						event.RowEvent.RowChanges[0].After,
					},
				})
				fmt.Printf("%v\n", res)
				// 5. Embed the text and add it to the vector store.
				upsertVector(store, res)
			default:
				fmt.Printf("event type: %v\n", event.Type)
			}
		}
	}
}

func init() {
	pflag.StringVar(&tableSchema, "table-schema", "", "The table schema.")
	pflag.StringVar(&tableName, "table-name", "", "The table name.")
	pflag.StringVar(&filterStatement, "filter-statement", "", "The filter statement.")
	pflag.StringVar(&gtid, "gtid", "", "The GTID.")
	pflag.StringVar(&weScaleUrl, "we-scale-url", "", "The WeScale URL.")
	pflag.StringVar(&embeddingModel, "embedding-model", "", "The embedding model.")
	pflag.StringVar(&embeddingUrl, "embedding-url", "", "The embedding URL.")
	pflag.StringVar(&vectorStoreUrl, "vector-store-url", "", "The vector store URL.")
	pflag.StringVar(&vectorStoreCollectionName, "vector-store-collection-name", "", "The vector store collection name.")
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
	if weScaleUrl == "" {
		return fmt.Errorf("we-scale-url is required")
	}
	if embeddingModel == "" {
		return fmt.Errorf("embedding-model is required")
	}
	if embeddingUrl == "" {
		return fmt.Errorf("embedding-url is required")
	}
	if vectorStoreUrl == "" {
		return fmt.Errorf("vector-store-url is required")
	}
	if vectorStoreCollectionName == "" {
		return fmt.Errorf("vector-store-collection-name is required")
	}
	return nil
}

func test() {
	tableSchema = "d1"
	tableName = "t1"
	filterStatement = "select * from t1"
	gtid = ""
	weScaleUrl = "localhost:15991"
	embeddingModel = "text-embedding-3-large"
	embeddingUrl = "https://api.gptsapi.net/v1"
	vectorStoreUrl = "http://127.0.0.1:6333/"
	vectorStoreCollectionName = "t1_vector"
}

func openWeScaleClient() (vtgateservice.VitessClient, func(), error) {
	conn, err := grpc.Dial(weScaleUrl, grpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to vtgate: %v", err)
	}
	client := vtgateservice.NewVitessClient(conn)
	closeFunc := func() {
		conn.Close()
	}
	return client, closeFunc, nil
}

func initVectorStore() vectorstores.VectorStore {
	if store != nil {
		return store
	}
	opts := []openai.Option{
		openai.WithEmbeddingModel(embeddingModel),
		openai.WithBaseURL(embeddingUrl),
	}
	llm, err := openai.New(opts...)
	if err != nil {
		log.Fatal(err)
	}

	e, err := embeddings.NewEmbedder(llm)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Qdrant vector store.
	url, err := url.Parse(vectorStoreUrl)
	if err != nil {
		log.Fatal(err)
	}
	s, err := qdrant.New(
		qdrant.WithURL(*url),
		qdrant.WithCollectionName(vectorStoreCollectionName),
		qdrant.WithEmbedder(e),
	)
	if err != nil {
		log.Fatal(err)
	}

	store = &s
	return store
}

func upsertVector(store vectorstores.VectorStore, result *sqltypes.Result) {
	// Convert the row values to a single string.
	text := ""
	for s, value := range result.Named().Row() {
		text += fmt.Sprintf("%s=%s\n", s, value.ToString())
	}

	// Add documents to the Qdrant vector store.
	_, err := store.AddDocuments(context.Background(), []schema.Document{
		{
			PageContent: text,
			Metadata:    map[string]any{},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
