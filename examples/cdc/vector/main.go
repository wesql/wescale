//go:build wasip1

/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	_ "github.com/stealthrocket/net/http"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/tmc/langchaingo/vectorstores/qdrant"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	cdc "github.com/wesql/wescale-cdc"
	"log"
	"net/http"
	"net/url"
)

var embeddingModel string
var embeddingUrl string
var vectorStoreType string
var vectorStoreUrl string
var vectorStoreCollectionName string

var store vectorstores.VectorStore

func main() {
	mockConfig()

	cc := cdc.NewCdcConsumer()
	cc.DialContextFunc = func(ctx context.Context, address string) (net.Conn, error) {
		return wasip1.DialContext(ctx, "tcp", address)
	}
	cc.Open()
	defer cc.Close()

	cc.Run()
}

func init() {

	flag.StringVar(&embeddingModel, "EMBEDDING_MODEL", "", "The embedding model.")
	flag.StringVar(&embeddingUrl, "EMBEDDING_URL", "", "The embedding URL.")
	flag.StringVar(&vectorStoreType, "VECTOR_STORE_TYPE", "", "The vector store type.")
	flag.StringVar(&vectorStoreUrl, "VECTOR_STORE_URL", "", "The vector store URL.")
	flag.StringVar(&vectorStoreCollectionName, "VECTOR_STORE_COLLECTION_NAME", "", "The vector store collection name.")

	cdc.SpiOpen = Open
	cdc.SpiLoadGTIDAndLastPK = loadGTIDAndLastPK
	cdc.SpiStoreGtidAndLastPK = storeGtidAndLastPK
	cdc.SpiStoreTableData = storeTableData
	cdc.SpiClose = Close
}

func Open(cc *cdc.CdcConsumer) {
	err := initVectorStore()
	if err != nil {
		log.Fatalf("failed to initialize the vector store: %v", err)
	}
}

func Close(cc *cdc.CdcConsumer) {
}

func loadGTIDAndLastPK(cc *cdc.CdcConsumer) (string, *querypb.QueryResult, error) {
	return "", nil, nil
}

func storeGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, cc *cdc.CdcConsumer) error {
	return nil
}

func storeTableData(resultList []*cdc.RowResult, cc *cdc.CdcConsumer) error {
	docList := make([]schema.Document, 0)
	for _, rowResult := range resultList {
		switch rowResult.RowType {
		case cdc.INSERT, cdc.UPDATE:

		case cdc.DELETE:
			// We Should Delete the row from the Qdrant vector store. But currently, we do nothing.
			continue
		}

		text := ""
		for s, value := range rowResult.After.Named().Row() {
			text += fmt.Sprintf("%s=%s\n", s, value.ToString())
		}
		docList = append(docList, schema.Document{
			PageContent: text,
			Metadata:    map[string]any{},
		})
	}

	// Add documents to the Qdrant vector store.
	_, err := store.AddDocuments(cc.Ctx, docList)
	if err != nil {
		log.Fatal(err)
	}

	return nil
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

func mockConfig() {
	cdc.DefaultConfig.TableSchema = "d1"
	cdc.DefaultConfig.SourceTableName = "t1"
	cdc.DefaultConfig.TargetTableName = "t2"
	cdc.DefaultConfig.FilterStatement = "select * from t1"
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"

	embeddingModel = "text-embedding-3-large"
	embeddingUrl = "https://api.gptsapi.net/v1"
	vectorStoreType = "qdrant"
	vectorStoreUrl = "http://127.0.0.1:6333/"
	vectorStoreCollectionName = "t1_vector"
}
