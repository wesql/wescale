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
	"strings"
)

var EmbeddingModel string
var EmbeddingUrl string

var VectorStoreType string
var VectorStoreUrl string
var VectorStoreCollectionName string
var VectorDistanceMethod string

var EmbeddingCols string
var MetaCols string
var EmbeddingColsMap map[string]bool
var MetaColsMap map[string]bool

const (
	VectorStoreTypeQdrant = "qdrant"
)

var Store vectorstores.VectorStore

func init() {
	cdc.RegisterStringVar(&EmbeddingModel, "EMBEDDING_MODEL", "", "The embedding model.")
	cdc.RegisterStringVar(&EmbeddingUrl, "EMBEDDING_URL", "", "The embedding URL.")
	cdc.RegisterStringVar(&VectorStoreType, "VECTOR_STORE_TYPE", "", "The vector Store type.")
	cdc.RegisterStringVar(&VectorStoreUrl, "VECTOR_STORE_URL", "", "The vector Store URL.")
	cdc.RegisterStringVar(&VectorStoreCollectionName, "VECTOR_STORE_COLLECTION_NAME", "", "The vector Store collection name.")
	cdc.RegisterStringVar(&VectorDistanceMethod, "VECTOR_DISTANCE_METHOD", "", "The vector distance method name.")
	cdc.RegisterStringVar(&EmbeddingCols, "EMBEDDING_COLS", "", "The name of columns to be embedded, if there are more than one, separate them by ','.")
	cdc.RegisterStringVar(&MetaCols, "META_COLS", "", "The name of columns to be recorded as metadata in vector database, if there are more than one, separate them by ','.")

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

func transferColsToMap() {
	// transfer embedding and metadata cols str to map
	EmbeddingColsMap = make(map[string]bool)
	EmbeddingCols = strings.ReplaceAll(EmbeddingCols, " ", "")
	EmbeddingColsSlice := strings.Split(EmbeddingCols, ",")
	for _, col := range EmbeddingColsSlice {
		EmbeddingColsMap[col] = true
	}
	MetaColsMap = make(map[string]bool)
	MetaCols = strings.ReplaceAll(MetaCols, " ", "")
	MetaColsSlice := strings.Split(MetaCols, ",")
	for _, col := range MetaColsSlice {
		MetaColsMap[col] = true
	}
}

func Open(cc *cdc.CdcConsumer) {
	err := initVectorStore(cc)
	if err != nil {
		cdc.SpiFatalf("failed to initialize the vector Store: %v", err)
	}

	transferColsToMap()
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
		metaData := make(map[string]any)
		for idx, value := range rowResult.After.Rows[0] {
			colName := rowResult.After.Fields[idx].Name
			if _, ok := EmbeddingColsMap[colName]; ok {
				text += fmt.Sprintf("%s=%s\n", colName, value.ToString())
			}
			if _, ok := MetaColsMap[colName]; ok {
				metaData[colName] = value.ToString()
			}
		}
		docList = append(docList, schema.Document{
			PageContent: text,
			Metadata:    metaData,
		})
	}

	// Add documents to the Qdrant vector Store.
	_, err := Store.AddDocuments(cc.Ctx, docList)
	if err != nil {
		cdc.SpiFatalf("failed to add documents to the vector Store: %v", err)
	}

	return nil
}

// todo cdc: we need to let the user to choose the embedding provider. Currently, we only support OpenAI. We need to add support for other providers.
// see github.com/tmc/langchaingo/embeddings for more providers.
func initEmbedding(cc *cdc.CdcConsumer) (*embeddings.EmbedderImpl, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return cc.DialContextFunc(ctx, addr)
		},
	}
	httpClient := &http.Client{Transport: tr}
	opts := []openai.Option{
		openai.WithEmbeddingModel(EmbeddingModel),
		openai.WithBaseURL(EmbeddingUrl),
		openai.WithHTTPClient(httpClient),
	}
	llm, err := openai.New(opts...)
	if err != nil {
		return nil, err
	}

	return embeddings.NewEmbedder(llm)
}

func initVectorStore(cc *cdc.CdcConsumer) error {
	if Store != nil {
		return nil
	}

	e, err := initEmbedding(cc)
	if err != nil {
		return err
	}

	switch VectorStoreType {
	case VectorStoreTypeQdrant:
		// Create a new Qdrant collection if it does not exist.
		qdrantUtil := &QdrantUtil{BaseURL: VectorStoreUrl}
		exists, err := qdrantUtil.CheckCollectionExists(VectorStoreCollectionName)
		if err != nil {
			return err
		}
		if !exists {
			err := qdrantUtil.CreateCollectionExists(VectorStoreCollectionName, VectorDistanceMethod, 3072)
			if err != nil {
				return err
			}
		}

		// Create a new Qdrant vector Store.
		url, err := url.Parse(VectorStoreUrl)
		if err != nil {
			return err
		}
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
	default:
		// todo cdc: we need to let the user to choose the vector Store. Currently, we only support Qdrant. We need to add support for other vector stores.
		// see github.com/tmc/langchaingo/vectorstores for more providers.
		return fmt.Errorf("supported vector Store type: %s", VectorStoreType)
	}

}
