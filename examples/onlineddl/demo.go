/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

func main() {
	continuousInsertData()
}

func continuousInsertData() {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:15306)/d1")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err.Error())
	}

	ctx, cancel := context.WithCancel(ctx)

	insertFunc := func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn.ExecContext(ctx, "insert into t1(c1, c2) values(null, 1)")
			}
		}
	}

	go insertFunc()
	go insertFunc()
	go insertFunc()
	go insertFunc()
	go insertFunc()

	time.Sleep(100 * time.Second)
	cancel()
}
