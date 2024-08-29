/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package vector

import (
	"context"
	"crypto/tls"
	"fmt"
	_ "github.com/stealthrocket/net/http"
	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/vectorstores"
	"github.com/tmc/langchaingo/vectorstores/qdrant"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	cdc "github.com/wesql/wescale-cdc"
	"net"
	"net/http"
	"net/url"
	"os"
)

var EmbeddingModel string
var EmbeddingUrl string
var VectorStoreType string
var VectorStoreUrl string
var VectorStoreCollectionName string

var Store vectorstores.VectorStore

func init() {
	cdc.RegisterStringVar(&EmbeddingModel, "EMBEDDING_MODEL", "", "The embedding model.")
	cdc.RegisterStringVar(&EmbeddingUrl, "EMBEDDING_URL", "", "The embedding URL.")
	cdc.RegisterStringVar(&VectorStoreType, "VECTOR_STORE_TYPE", "", "The vector Store type.")
	cdc.RegisterStringVar(&VectorStoreUrl, "VECTOR_STORE_URL", "", "The vector Store URL.")
	cdc.RegisterStringVar(&VectorStoreCollectionName, "VECTOR_STORE_COLLECTION_NAME", "", "The vector Store collection name.")

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
	err := initVectorStore(cc)
	if err != nil {
		cdc.SpiFatalf("failed to initialize the vector Store: %v", err)
	}
}

func Close(cc *cdc.CdcConsumer) {
}

func LoadGTIDAndLastPK(cc *cdc.CdcConsumer) (string, *querypb.QueryResult, error) {
	return "", nil, nil
}

func StoreGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, cc *cdc.CdcConsumer) error {
	return nil
}

func StoreTableData(resultList []*cdc.RowResult, cc *cdc.CdcConsumer) error {
	docList := make([]schema.Document, 0)
	for _, rowResult := range resultList {
		switch rowResult.RowType {
		case cdc.INSERT, cdc.UPDATE:

		case cdc.DELETE:
			// We Should Delete the row from the Qdrant vector Store. But currently, we do nothing.
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

	// Add documents to the Qdrant vector Store.
	_, err := Store.AddDocuments(cc.Ctx, docList)
	if err != nil {
		cdc.SpiFatalf("failed to add documents to the vector Store: %v", err)
	}

	return nil
}

func initVectorStore(cc *cdc.CdcConsumer) error {
	if Store != nil {
		return nil
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return cc.DialContextFunc(ctx, addr)
		},
	}
	httpClient := &http.Client{Transport: tr}
	// todo cdc: we need to let the user to choose the embedding provider. Currently, we only support OpenAI. We need to add support for other providers.
	// see github.com/tmc/langchaingo/embeddings for more providers.
	opts := []openai.Option{
		openai.WithEmbeddingModel(EmbeddingModel),
		openai.WithBaseURL(EmbeddingUrl),
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

	// Create a new Qdrant vector Store.
	url, err := url.Parse(VectorStoreUrl)
	if err != nil {
		return err
	}
	// todo cdc: we need to let the user to choose the vector Store. Currently, we only support Qdrant. We need to add support for other vector stores.
	// see github.com/tmc/langchaingo/vectorstores for more providers.
	s, err := qdrant.New(
		qdrant.WithURL(*url),
		qdrant.WithCollectionName(VectorStoreCollectionName),
		qdrant.WithEmbedder(e),
	)
	if err != nil {
		return err
	}

	Store = &s
	return nil
}
