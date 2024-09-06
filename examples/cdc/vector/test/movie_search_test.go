/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	cdc "github.com/wesql/wescale-cdc"
	"github.com/wesql/wescale/examples/cdc/vector"
	"io/ioutil"
	"testing"
)

const (
	host              = "127.0.0.1"
	port              = 15306
	moviesTableSchema = "d1"
)

func TestInitMovieTable(t *testing.T) {
	dsn := fmt.Sprintf("(%s:%d)/%s?multiStatements=true", host, port, moviesTableSchema)
	db, err := sql.Open("mysql", dsn)
	assert.Nil(t, err)
	defer db.Close()

	sqlFile := "init_movies_table.sql"
	content, err := ioutil.ReadFile(sqlFile)
	assert.Nil(t, err)

	_, err = db.Exec(string(content))
	assert.Nil(t, err)
}

func TestMovieSearch(t *testing.T) {
	mockConfig()
	cc := cdc.NewCdcConsumer()
	vector.Open(cc)
	ctx := context.Background()
	rest, err := vector.Store.SimilaritySearch(ctx, "I want to watch a movie about computer programmers.", 5)
	assert.Nil(t, err)
	for _, movie := range rest {
		fmt.Printf("search score: %v,\nmovie title: %v,\nmovie overview: %v,\nmovie genres: %v,\nmoive cast: %v\nmovie producer: %v\n\n",
			movie.Score, movie.Metadata["title"], movie.Metadata["overview"], movie.Metadata["genres"], movie.Metadata["producer"], movie.Metadata["cast"])
	}
}

func mockConfig() {
	cdc.DefaultConfig.TableSchema = "d1"
	cdc.DefaultConfig.SourceTableName = "movies"
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"

	vector.EmbeddingModel = "text-embedding-3-large"
	vector.EmbeddingUrl = "https://api.gptsapi.net/v1"
	vector.VectorStoreType = vector.VectorStoreTypeQdrant
	vector.VectorStoreUrl = "http://127.0.0.1:6333/"
	vector.VectorStoreCollectionName = "movies_vector"
	vector.VectorDistanceMethod = "Dot"
	vector.EmbeddingCols = "title,overview,genres,"
	vector.MetaCols = "title,overview,genres,producer,cast"
}
