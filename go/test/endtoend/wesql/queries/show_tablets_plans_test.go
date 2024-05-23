/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/test/endtoend/utils"

	"github.com/wesql/wescale/go/mysql"
)

func aliasCount(qr *sqltypes.Result) int {
	set := make(map[string]bool)
	for _, row := range qr.Named().Rows {
		set[row["tablet_alias"].ToString()] = true
	}
	return len(set)
}

func getAlias(qr *sqltypes.Result) []string {
	set := make(map[string]bool)
	for _, row := range qr.Named().Rows {
		set[row["tablet_alias"].ToString()] = true
	}

	var rst []string
	for k := range set {
		rst = append(rst, k)
	}
	return rst
}

func TestShowTabletsPlansFilter(t *testing.T) {
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {

		// there are already query plans on primary and replica tablets due to test cases executed before,
		// but we still generate some query plans on primary and replica tablets here
		utils.Exec(t, conn, "create database showTabletsPlansTest")
		defer utils.Exec(t, conn, "drop database showTabletsPlansTest")

		utils.Exec(t, conn, "create table showTabletsPlansTest.t (c1 int, c2 int, primary key(c1))")
		utils.Exec(t, conn, "insert into showTabletsPlansTest.t(c1, c2) values (2, 2)")

		//generate query plans on replica tablets by routing
		utils.Exec(t, conn, "set session read_write_splitting_policy='random'")
		utils.Exec(t, conn, "set session enable_read_write_splitting_for_read_only_txn=true")
		utils.Exec(t, conn, "start transaction read only")
		utils.Exec(t, conn, "select * from showTabletsPlansTest.t")
		utils.Exec(t, conn, "commit")

		// start testing show tablets plans filter
		qr, err := conn.ExecuteFetch("show tablets_plans", 1000, true)
		assert.Equal(t, nil, err)
		assert.Greater(t, aliasCount(qr), 1)

		aliasArray := getAlias(qr)
		log.Printf("aliasArray: %v,len %d", aliasArray, len(aliasArray))
		for _, alias := range aliasArray {
			fmt.Printf("alias is %s", alias)
			print(fmt.Sprintf("show tablets_plans like '%s'", alias))
			qr, err = conn.ExecuteFetch(fmt.Sprintf("show tablets_plans like '%s'", alias), 1000, true)
			assert.Equal(t, nil, err)
			assert.Equal(t, 1, aliasCount(qr))

			qr, err = conn.ExecuteFetch(fmt.Sprintf("show tablets_plans where alias = %s", alias), 1000, true)
			assert.Equal(t, nil, err)
			assert.Equal(t, 1, aliasCount(qr))
		}

		qr, err = conn.ExecuteFetch("show tablets_plans like ''", 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, aliasCount(qr))

		qr, err = conn.ExecuteFetch("show tablets_plans like 'zone1'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, aliasCount(qr))

		qr, err = conn.ExecuteFetch("show tablets_plans like '%zone1'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, aliasCount(qr))

		qr, err = conn.ExecuteFetch("show tablets_plans like '%zone1%'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Greater(t, aliasCount(qr), 1)

		qr, err = conn.ExecuteFetch("show tablets_plans like 'zone1%'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Greater(t, aliasCount(qr), 1)

		qr, err = conn.ExecuteFetch("show tablets_plans like 'zone1%%'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Greater(t, aliasCount(qr), 1)

		qr, err = conn.ExecuteFetch("show tablets_plans where alias = 'zone1%'", 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, aliasCount(qr))

		_, err = conn.ExecuteFetch("show tablets_plans where alias2 = 'zone1%'", 1000, true)
		assert.NotEqual(t, nil, err)

		_, err = conn.ExecuteFetch("show tablets_plans where 1 = 1", 1000, true)
		assert.NotEqual(t, nil, err)

		_, err = conn.ExecuteFetch("show tablets_plans where abc", 1000, true)
		assert.NotEqual(t, nil, err)
	})
}
