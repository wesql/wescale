package main

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
)

func main() {
	dsn := "root@(127.0.0.1:17101)/d1"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// 比较 t1 和 t2 表的内容
	equal, err := compareTables(db, "t1", "t2")
	if err != nil {
		log.Fatal(err)
	}

	if equal {
		fmt.Println("Tables t1 and t2 are identical.")
	} else {
		fmt.Println("Tables t1 and t2 are not identical.")
	}
}

func compareTables(db *sql.DB, table1, table2 string) (bool, error) {
	rows1, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY c1", table1)) // 假设有主键id
	if err != nil {
		return false, err
	}
	defer rows1.Close()

	rows2, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY c1", table2)) // 假设有主键id
	if err != nil {
		return false, err
	}
	defer rows2.Close()

	cols1, err := rows1.Columns()
	if err != nil {
		return false, err
	}

	cols2, err := rows2.Columns()
	if err != nil {
		return false, err
	}

	if len(cols1) != len(cols2) {
		return false, nil
	}

	for i := 0; i < len(cols1); i++ {
		if cols1[i] != cols2[i] {
			return false, nil
		}
	}

	values1 := make([]interface{}, len(cols1))
	values2 := make([]interface{}, len(cols1))
	columns1 := make([]interface{}, len(cols1))
	columns2 := make([]interface{}, len(cols1))

	for i := range values1 {
		columns1[i] = &values1[i]
		columns2[i] = &values2[i]
	}

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
				log.Printf("val1 %v not equal val2 %v", val1, val2)
				return false, nil
			}
		}
	}

	if rows1.Next() || rows2.Next() {
		log.Printf("rows not equal")
		return false, nil
	}

	return true, nil
}
