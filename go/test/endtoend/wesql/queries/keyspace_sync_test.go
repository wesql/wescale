/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package queries

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestEnsureKeyspaceForSystemDbExists(t *testing.T) {
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {
		timeout := time.After(20 * time.Second)
		for {
			select {
			case <-timeout:
				t.Fatalf("timeout waiting for system databases to be created in MySQL")
				return
			default:
				allExists := true
				allExists = allExists && utils.TestDatabaseExists(t, conn, "information_schema")
				allExists = allExists && utils.TestDatabaseExists(t, conn, "mysql")
				allExists = allExists && utils.TestDatabaseExists(t, conn, "sys")
				allExists = allExists && utils.TestDatabaseExists(t, conn, "performance_schema")
				if allExists {
					return
				}
			}
		}

	})
}

func TestKeyspaceMetaSyncedAfterCreatingDropingDatabaseInMySQL(t *testing.T) {
	backendPrimaryMysqlConn := getBackendPrimaryMysqlConn()
	defer backendPrimaryMysqlConn.Close()

	// create a database in MySQL
	dbName := "keyspace_meta_created_test"
	utils.Exec(t, backendPrimaryMysqlConn, "create database "+dbName)
	utils.AssertDatabaseExists(t, backendPrimaryMysqlConn, dbName)

	// make sure the keyspace meta is created in WeScale
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {
		timeout := time.After(20 * time.Second)
		// loop forever: exec 'show database like dbName' util the database is created in MySQL, or timeout
		for {
			select {
			case <-timeout:
				t.Fatalf("timeout waiting for database %s to be created in MySQL", dbName)
				return
			default:
				// check if the database is created in MySQL
				qr := utils.Exec(t, conn, fmt.Sprintf("show databases like '%s'", dbName))
				if len(qr.Rows) == 1 {
					return
				}
			}
		}
	})

	// drop the database in MySQL
	utils.Exec(t, backendPrimaryMysqlConn, "drop database "+dbName)
	utils.AssertDatabaseNotExists(t, backendPrimaryMysqlConn, dbName)

	// make sure the keyspace meta is dropped in WeScale
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {
		timeout := time.After(20 * time.Second)
		for {
			select {
			case <-timeout:
				t.Fatalf("timeout waiting for database %s to be created in MySQL", dbName)
				return
			default:
				// check if the database is created in MySQL
				qr := utils.Exec(t, conn, fmt.Sprintf("show databases like '%s'", dbName))
				if len(qr.Rows) == 0 {
					return
				}
			}
		}
	})
}
