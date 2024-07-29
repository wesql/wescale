//go:build wasip1

/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/spf13/pflag"
	"github.com/tmc/langchaingo/vectorstores"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"

	"github.com/wesql/sqlparser/go/sqltypes"
	binlogdatapb "github.com/wesql/sqlparser/go/vt/proto/binlogdata"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	topodatapb "github.com/wesql/sqlparser/go/vt/proto/topodata"
	vtgatepb "github.com/wesql/sqlparser/go/vt/proto/vtgate"
	"github.com/wesql/sqlparser/go/vt/proto/vtgateservice"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores/qdrant"

	_ "github.com/stealthrocket/net/http"
	"github.com/stealthrocket/net/wasip1"
)

var tableSchema string
var tableName string
var filterStatement string
var gtid string

var wescaleURL string

var embeddingModel string
var embeddingUrl string

var vectorStoreType string
var vectorStoreUrl string
var vectorStoreCollectionName string

var store vectorstores.VectorStore

// GOOS=wasip1 GOARCH=wasm go build -o demo.wasm demo.go
// wasirun --env OPENAI_API_KEY=xxx demo.wasm

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
	err = initVectorStore()
	if err != nil {
		log.Fatalf("error: %v", err)
	}

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
	if err != nil {
		log.Fatalf("failed to create vstream: %v", err)
	}

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
				fields = event.FieldEvent.Fields
			case binlogdatapb.VEventType_ROW:
				// 5. Embed the text and add it to the vector store.
				res := sqltypes.CustomProto3ToResult(fields, &querypb.QueryResult{
					Fields: fields,
					Rows: []*querypb.Row{
						event.RowEvent.RowChanges[0].After,
					},
				})
				upsertVector(store, res)
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
	pflag.StringVar(&embeddingModel, "EMBEDDING_MODEL", "", "The embedding model.")
	pflag.StringVar(&embeddingUrl, "EMBEDDING_URL", "", "The embedding URL.")
	pflag.StringVar(&vectorStoreType, "VECTOR_STORE_TYPE", "", "The vector store type.")
	pflag.StringVar(&vectorStoreUrl, "VECTOR_STORE_URL", "", "The vector store URL.")
	pflag.StringVar(&vectorStoreCollectionName, "VECTOR_STORE_COLLECTION_NAME", "", "The vector store collection name.")
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
	if embeddingModel == "" {
		return fmt.Errorf("embedding-model is required")
	}
	if embeddingUrl == "" {
		return fmt.Errorf("embedding-url is required")
	}
	if vectorStoreType == "" {
		return fmt.Errorf("vector-store-type is required")
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
	wescaleURL = "127.0.0.1:15991"
	embeddingModel = "text-embedding-3-large"
	embeddingUrl = "https://api.gptsapi.net/v1"
	vectorStoreType = "qdrant"
	vectorStoreUrl = "http://127.0.0.1:6333/"
	vectorStoreCollectionName = "t1_vector"
}

func openWeScaleClient() (vtgateservice.VitessClient, func(), error) {
	conn, err := grpc.Dial(wescaleURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return wasip1.DialContext(ctx, "tcp", address)
		}),
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

func initVectorStore() error {
	if store != nil {
		return nil
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext:     wasip1.DialContext,
	}
	httpClient := &http.Client{Transport: tr}
	// todo cdc: we need to let the user to choose the embedding provider. Currently, we only support OpenAI. We need to add support for other providers.
	// see github.com/tmc/langchaingo/embeddings for more providers.
	opts := []openai.Option{
		openai.WithEmbeddingModel(embeddingModel),
		openai.WithBaseURL(embeddingUrl),
		openai.WithHTTPClient(httpClient),
	}
	llm, err := openai.New(opts...)
	if err != nil {
		return err
	}

	e, err := embeddings.NewEmbedder(llm)
	if err != nil {
		return err
	}

	// Create a new Qdrant vector store.
	url, err := url.Parse(vectorStoreUrl)
	if err != nil {
		return err
	}
	// todo cdc: we need to let the user to choose the vector store. Currently, we only support Qdrant. We need to add support for other vector stores.
	// see github.com/tmc/langchaingo/vectorstores for more providers.
	s, err := qdrant.New(
		qdrant.WithURL(*url),
		qdrant.WithCollectionName(vectorStoreCollectionName),
		qdrant.WithEmbedder(e),
	)
	if err != nil {
		return err
	}

	store = &s
	return nil
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
