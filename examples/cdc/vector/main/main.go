/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package main

import (
	cdc "github.com/wesql/wescale-cdc"
	"github.com/wesql/wescale/examples/cdc/vector"
)

/*
*
* Mock data:
* Before running the program, please create the following table and insert some data into it.
create table t1 (c1 int primary key auto_increment, c2 text);

* Once the program is running, you can run the following SQLs to test the program.
insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
insert into t1 (c2) values ('I want you to act as an interviewer.');
insert into t1 (c2) values ('I want you to act as an engineer.');
delete from t1 where c2 = 'I want you to act as an English translator, spelling corrector and improver.';
update t1 set c1 = 12345 where c2 = 'I want you to act as an interviewer.';

* You can compare the data in the source table and the target table to see if the program works as expected.
*/
func main() {
	mockConfig()

	cc := cdc.NewCdcConsumer()
	cc.Open()
	defer cc.Close()

	cc.Run()
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
