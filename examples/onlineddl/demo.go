/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/throttler"
)

func main() {
	err := submitVReplication("d1", "d3", []string{"t1"})
	if err != nil {
		fmt.Printf("err: %v", err)
	}
}

func submitVReplication(sourceDb, targetDb string, tables []string) error {
	bls := &binlogdatapb.BinlogSource{
		Keyspace: sourceDb,
		Shard:    "0",
		Filter:   &binlogdatapb.Filter{},
	}
	for _, table := range tables {
		bls.Filter.Rules = append(bls.Filter.Rules,
			&binlogdatapb.Rule{
				Match:  table,
				Filter: "select `c1` as `c1`, `c2` as `c2`, `c3` as `c3` from `t1`",
			})
	}
	val := sqltypes.NewVarBinary(fmt.Sprintf("%v", bls))
	var sqlEscaped bytes.Buffer
	val.EncodeSQL(&sqlEscaped)

	query := binlogplayer.CreateVReplication("Resharding", bls, "", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823, "d3", 0, 0, false)
	fmt.Println("======query======\n", query)

	cell := "zone1-100"
	cmd := fmt.Sprintf("vtctlclient --server localhost:15999 VReplicationExec '%s' \"%s\"", cell, query)
	fmt.Println("======cmd======\n", cmd)

	err := executeBash(cmd)
	if err != nil {
		return err
	}
	return nil
}

func executeBash(cmdStr string) error {
	cmd := exec.Command("/bin/bash", "-c", cmdStr)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("Output: %q\n", out.String())
	return nil
}
