/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

func execute(dsn string, query string) (sql.Result, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	return conn.ExecContext(ctx, query)
}

func query(dsn string, query string) (*sql.Rows, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	return conn.QueryContext(ctx, query)
}

func createDatabaseIfNotExists(dsn string, dbName string) {
	execute(dsn, "create database if not exists "+dbName)
}

func executeOnlineDDL(dsn string, ddlStrategy, ddl string) (string, error) {
	execute(dsn, "set @@ddl_strategy='"+ddlStrategy+"'")

	rows, err := query(dsn, ddl)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	rows.Next()
	var uuid string
	err = rows.Scan(&uuid)
	if err != nil {
		return "", err
	}
	return uuid, nil
}

func executeOnlineDDLThenWaitForCompletion(dsn string, ddlStrategy, ddl string) error {
	uuid, err := executeOnlineDDL(dsn, ddlStrategy, ddl)
	if err != nil {
		return err
	}
	for {
		rows, err := query(dsn, fmt.Sprintf("select migration_status from mysql.schema_migrations where migration_uuid = '%s'", uuid))
		if err != nil {
			return err
		}
		defer rows.Close()

		if !rows.Next() {
			return fmt.Errorf("migration %s not found", uuid)
		}
		var migrationStatus string
		err = rows.Scan(&migrationStatus)
		if err != nil {
			return err
		}
		if migrationStatus == "complete" {
			return nil
		}
		if migrationStatus == "queued" || migrationStatus == "ready" || migrationStatus == "running" {
			continue
		}
		return fmt.Errorf("migration %s failed", uuid)
	}
}

func continuousInsertData(dsn string, insertSQL string, parallelism int, duration time.Duration) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		panic(err.Error())
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	insertFunc := func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			default:
				conn.ExecContext(cancelCtx, insertSQL)
			}
		}
	}

	for i := 1; i <= parallelism; i++ {
		go insertFunc()
	}
	time.Sleep(duration)
	cancel()
}
