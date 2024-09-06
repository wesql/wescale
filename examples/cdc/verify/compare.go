/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"reflect"
	"strings"
)

const (
	host         = "127.0.0.1"
	port         = 15306
	table1Schema = "d1"
	table2Schema = "d1"
	table1       = "accounts1"
	table2       = "accounts2"
)

func main() {
	dsn := fmt.Sprintf("(%s:%d)/%s", host, port, table1Schema)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	equal, err := compareTables(db, table1Schema, table1, table2Schema, table2)
	if err != nil {
		log.Fatal(err)
	}

	if equal {
		fmt.Printf("Tables %s.%s and %s.%s are identical.", table1Schema, table1, table2Schema, table2)
	} else {
		fmt.Println("Tables %s.%s and %s.%s are not identical.", table1Schema, table1, table2Schema, table2)
	}
}

const batchSize = 10000 // 每次查询 1 万条记录

func getPrimaryKeyColumn(db *sql.DB, tableSchema, tableName string) ([]string, error) {
	query := `
        SELECT COLUMN_NAME 
        FROM information_schema.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
    `
	rows, err := db.Query(query, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key for table %s: %v", tableName, err)
	}
	primaryKeys := make([]string, 0)
	for rows.Next() {
		var primaryKey string
		if err := rows.Scan(&primaryKey); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, primaryKey)
	}
	return primaryKeys, nil
}

type colInfo struct {
	Type      string
	IsPrimary bool
}

func getTableColInfo(db *sql.DB, tableName string) (map[string]*colInfo, error) {
	query := `
        SELECT COLUMN_NAME, COLUMN_TYPE,COLUMN_KEY = 'PRI' AS IS_PRIMARY
        FROM information_schema.COLUMNS 
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
    `
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}
	defer rows.Close()

	schema := make(map[string]*colInfo)
	for rows.Next() {
		var columnName, columnType string
		var isPrimary bool
		if err := rows.Scan(&columnName, &columnType, &isPrimary); err != nil {
			return nil, err
		}
		schema[columnName] = &colInfo{
			Type:      columnType,
			IsPrimary: isPrimary,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schema, nil
}

func compareTableSchema(db *sql.DB, table1, table2 string) (bool, error) {
	table1ColInfos, err := getTableColInfo(db, table1)
	if err != nil {
		return false, err
	}

	table2ColInfos, err := getTableColInfo(db, table2)
	if err != nil {
		return false, err
	}

	if len(table1ColInfos) != len(table2ColInfos) {
		return false, nil
	}

	for table1ColName, table1ColInfo := range table1ColInfos {
		if table2ColInfo, ok := table2ColInfos[table1ColName]; !ok || table1ColInfo.Type != table2ColInfo.Type ||
			table1ColInfo.IsPrimary != table2ColInfo.IsPrimary {
			log.Printf("Schema mismatch in column %s: (%s,%v) vs (%s.%v)", table1ColName, table1ColInfo.Type, table1ColInfo.IsPrimary, table2ColInfo.Type, table2ColInfo.IsPrimary)
			return false, nil
		}
	}

	return true, nil
}

func fetchBatch(db *sql.DB, tableSchema, table, primaryKey string, offset int) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d", tableSchema, table, primaryKey, batchSize, offset)
	return db.Query(query)
}

func compareTables(db *sql.DB, table1Schema, table1, table2Schema, table2 string) (bool, error) {
	schemaEqual, err := compareTableSchema(db, table1, table2)
	if err != nil {
		return false, err
	}
	if !schemaEqual {
		log.Println("Table schemas are not the same")
		return false, nil
	}

	primaryKeys, err := getPrimaryKeyColumn(db, table1Schema, table1)
	if err != nil {
		return false, err
	}
	primaryKeyStr := strings.Join(primaryKeys, ",")

	offset := 0
	for {
		rows1, err := fetchBatch(db, table1Schema, table1, primaryKeyStr, offset)
		if err != nil {
			return false, err
		}
		defer rows1.Close()

		rows2, err := fetchBatch(db, table2Schema, table2, primaryKeyStr, offset)
		if err != nil {
			return false, err
		}
		defer rows2.Close()

		cols1, err := rows1.Columns()
		if err != nil {
			return false, err
		}

		values1 := make([]interface{}, len(cols1))
		values2 := make([]interface{}, len(cols1))
		columns1 := make([]interface{}, len(cols1))
		columns2 := make([]interface{}, len(cols1))

		for i := range values1 {
			columns1[i] = &values1[i]
			columns2[i] = &values2[i]
		}

		rowsCount := 0

		for rows1.Next() && rows2.Next() {
			if err := rows1.Scan(columns1...); err != nil {
				return false, err
			}
			if err := rows2.Scan(columns2...); err != nil {
				return false, err
			}

			for i := range values1 {
				val1 := values1[i]
				val2 := values2[i]

				if !reflect.DeepEqual(val1, val2) {
					log.Printf("Value mismatch at column %d: %v (table1) != %v (table2)", i, val1, val2)
					return false, nil
				}
			}
			rowsCount += 1
		}

		if rows1.Next() || rows2.Next() {
			log.Printf("Row count mismatch between the tables")
			return false, nil
		}
		offset += rowsCount
		if rowsCount < batchSize {
			break
		}
	}

	log.Printf("rows number is %d", offset)
	return true, nil
}
