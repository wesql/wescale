/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"context"
	"fmt"
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

var store *qdrant.Store

// create table t1 (c1 int primary key auto_increment, c2 text);
// insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
// insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
// insert into t1 (c2) values ('I want you to act as an interviewer.');
func main() {

	initVectorStore()

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "d1",
			Shard:    "0",
			//Gtid:     "current",
			Gtid: "",
		}}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	// 连接到 vtgate 服务
	conn, err := grpc.Dial("127.0.0.1:15991", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect to vtgate: %v", err)
	}
	defer conn.Close()

	// 创建 vtgate 客户端
	client := vtgateservice.NewVitessClient(conn)
	defer conn.Close()
	flags := &vtgatepb.VStreamFlags{
		//MinimizeSkew:      false,
		//HeartbeatInterval: 60, //seconds
	}
	req := &vtgatepb.VStreamRequest{
		TabletType: topodatapb.TabletType_PRIMARY,
		Vgtid:      vgtid,
		Filter:     filter,
		Flags:      flags,
	}
	reader, err := client.VStream(context.Background(), req)

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
				upsertVector(*store, res)
			default:
				fmt.Printf("event type: %v\n", event.Type)
			}
		}
	}
}

func initVectorStore() *qdrant.Store {
	if store != nil {
		return store
	}
	opts := []openai.Option{
		openai.WithModel("gpt-3.5-turbo-0125"),
		openai.WithEmbeddingModel("text-embedding-3-large"),
		openai.WithBaseURL("https://api.gptsapi.net/v1"),
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
	url, err := url.Parse("http://127.0.0.1:6333/")
	if err != nil {
		log.Fatal(err)
	}
	s, err := qdrant.New(
		qdrant.WithURL(*url),
		qdrant.WithCollectionName("t1_vector"),
		qdrant.WithEmbedder(e),
	)
	if err != nil {
		log.Fatal(err)
	}

	store = &s
	return store
}

func upsertVector(store qdrant.Store, result *sqltypes.Result) {
	// Convert the row values to a single string.
	text := ""
	for s, value := range result.Named().Row() {
		text += fmt.Sprintf("%s=%s\n", s, value.ToString())
	}

	// Add documents to the Qdrant vector store.
	_, err := store.AddDocuments(context.Background(), []schema.Document{
		{
			PageContent: text,
			Metadata: map[string]any{
				"area": 2342,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
