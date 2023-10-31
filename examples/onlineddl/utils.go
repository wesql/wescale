package main

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func createDatabaseIfNotExists(dsn string, dbName string) {
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

	conn.ExecContext(ctx, "create database if not exists "+dbName)
}

func executeOnlineDDL(dsn string, ddlStrategy, ddl string) string {
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

	conn.ExecContext(ctx, "set @@ddl_strategy='"+ddlStrategy+"'")
	rows, err := conn.QueryContext(ctx, ddl)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()

	rows.Next()
	var uuid string
	err = rows.Scan(&uuid)
	if err != nil {
		panic(err.Error())
	}
	return uuid
}
