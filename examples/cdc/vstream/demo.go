/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// create table t1 (c1 int primary key auto_increment, c2 int);
// insert into t1 (c2) values (1);
func main() {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "d1",
			Shard:    "0",
			Gtid:     "current",
		}}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	conn, err := vtgateconn.Dial(context.Background(), "localhost:15991")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	flags := &vtgatepb.VStreamFlags{
		//MinimizeSkew:      false,
		//HeartbeatInterval: 60, //seconds
	}
	reader, err := conn.VStream(context.Background(), topodatapb.TabletType_PRIMARY, vgtid, filter, flags)

	var fields []*querypb.Field
	for {
		eventList, err := reader.Recv()
		if err == io.EOF {
			fmt.Printf("stream ended\n")
			return
		}
		if err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		for _, event := range eventList {
			switch event.Type {
			case binlogdatapb.VEventType_FIELD:
				fmt.Printf("%v\n", event.FieldEvent.Fields)
				fields = event.FieldEvent.Fields
			case binlogdatapb.VEventType_ROW:
				vals := sqltypes.MakeRowTrusted(fields, event.RowEvent.RowChanges[0].After)
				fmt.Printf("%v\n", event.RowEvent.RowChanges)
				fmt.Printf("%v\n", vals)
			default:
			}
		}
	}
}
