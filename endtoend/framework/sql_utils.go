package framework

import (
	"database/sql"
	"fmt"
)

func CreateDatabaseIfNotExists(db *sql.DB, dbName string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
	return err
}

func DropDatabaseIfExists(db *sql.DB, dbName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	return err
}

func ExecuteSqlScript(db *sql.DB, sqlScript string) error {
	_, err := db.Exec(sqlScript)
	return err
}
