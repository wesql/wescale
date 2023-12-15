/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package branch

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func insertUsers(dbConn *mysql.Conn, end int) error {
	start := 1
	batchSize := 1000

	for start <= end {
		batchEnd := start + batchSize - 1
		if batchEnd > end {
			batchEnd = end
		}

		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("INSERT INTO branch_source.user (id, name) VALUES ")

		comma := ""
		for i := start; i <= batchEnd; i++ {
			randomNumber := rand.Intn(999999) + 1
			sqlBuilder.WriteString(fmt.Sprintf("%s(%d, 'user%d')", comma, i, randomNumber))
			comma = ", "
		}

		_, err := dbConn.ExecuteFetch(sqlBuilder.String(), -1, false)
		if err != nil {
			return err
		}

		start = batchEnd + 1
	}

	return nil
}
func insertCustomer(dbConn *mysql.Conn, end int) error {
	start := 1
	batchSize := 1000

	for start <= end {
		batchEnd := start + batchSize - 1
		if batchEnd > end {
			batchEnd = end
		}

		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("INSERT INTO branch_source.customer (customer_id, email) VALUES ")

		comma := ""
		for i := start; i <= batchEnd; i++ {
			randomNumber := rand.Intn(999999) + 1
			sqlBuilder.WriteString(fmt.Sprintf("%s(%d, 'user%d@domain.com')", comma, i, randomNumber))
			comma = ", "
		}

		_, err := dbConn.ExecuteFetch(sqlBuilder.String(), -1, false)
		if err != nil {
			return err
		}

		start = batchEnd + 1
	}

	return nil
}
func insertProduct(dbConn *mysql.Conn, end int) error {
	start := 1
	batchSize := 1000

	for start <= end {
		batchEnd := start + batchSize - 1
		if batchEnd > end {
			batchEnd = end
		}

		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("INSERT INTO branch_source.product (sku, description, price) VALUES ")

		comma := ""
		for i := start; i <= batchEnd; i++ {
			sqlBuilder.WriteString(fmt.Sprintf("%s('SKU-%d', 'product description', %d)", comma, i, i))
			comma = ", "
		}

		_, err := dbConn.ExecuteFetch(sqlBuilder.String(), -1, false)
		if err != nil {
			return err
		}

		start = batchEnd + 1
	}

	return nil
}
func insertCorder(dbConn *mysql.Conn, end int, customerCount, productCount int) error {
	start := 1
	batchSize := 1000

	for start <= end {
		batchEnd := start + batchSize - 1
		if batchEnd > end {
			batchEnd = end
		}

		var sqlBuilder strings.Builder
		sqlBuilder.WriteString("INSERT INTO branch_source.corder (order_id, customer_id, sku, price) VALUES ")

		comma := ""
		for i := start; i <= batchEnd; i++ {
			randomCustomerID := rand.Intn(customerCount) + 1
			randomSkuID := rand.Intn(productCount) + 1
			sqlBuilder.WriteString(fmt.Sprintf("%s(%d, %d, 'SKU-%d', %d)", comma, i, randomCustomerID, randomSkuID, randomSkuID))
			comma = ", "
		}

		_, err := dbConn.ExecuteFetch(sqlBuilder.String(), -1, false)
		if err != nil {
			return err
		}

		start = batchEnd + 1
	}

	return nil
}

func TestInitTable(t *testing.T) {
	ctx := context.Background()
	var err error
	mysqlConn, err = mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	createSourceDatabase := "create database if not exists branch_source"
	createProductTable := `create table if not exists branch_source.product(
                                sku varchar(128),
                                description varchar(128),
                                price bigint,
                                primary key(sku)
                              ) ENGINE=InnoDB;`
	createCustomerTable := `create table if not exists branch_source.customer(
							customer_id bigint not null auto_increment,
							email varchar(128),
							primary key(customer_id)
						) ENGINE=InnoDB;`
	createCorderTable := `create table if not exists branch_source.corder(
                                order_id bigint not null auto_increment,
                                customer_id bigint,
                                sku varchar(128),
                                price bigint,
                                primary key(order_id)
                              ) ENGINE=InnoDB;`
	createUserTable := `CREATE TABLE if not exists branch_source.user (
                                  id INT AUTO_INCREMENT PRIMARY KEY auto_increment,
                                  name VARCHAR(255) NOT NULL
                              ) ENGINE=InnoDB;`
	_, err = mysqlConn.ExecuteFetch(createSourceDatabase, 1, false)
	require.Nil(t, err)
	_, err = mysqlConn.ExecuteFetch(createProductTable, 1, false)
	require.Nil(t, err)
	_, err = mysqlConn.ExecuteFetch(createCustomerTable, 1, false)
	require.Nil(t, err)
	_, err = mysqlConn.ExecuteFetch(createUserTable, 1, false)
	require.Nil(t, err)
	_, err = mysqlConn.ExecuteFetch(createCorderTable, 1, false)
	require.Nil(t, err)
	err = insertUsers(mysqlConn, userCount)
	require.Nil(t, err)
	err = insertCustomer(mysqlConn, customerCount)
	require.Nil(t, err)
	err = insertProduct(mysqlConn, productCount)
	require.Nil(t, err)
	err = insertCorder(mysqlConn, corderCount, customerCount, productCount)
	require.Nil(t, err)
}

func CleanupDatabase(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("drop database if exists branch_target", -1, false)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("delete from mysql.vreplication where 1=1", -1, false)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("delete from mysql.branch_jobs where 1=1", -1, false)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("delete from mysql.branch_table_rules where 1=1", -1, false)
	require.Nil(t, err)
}

func RequireVRplicationExist(t *testing.T, workflow string) {
	result, err := mysqlConn.ExecuteFetch(fmt.Sprintf("SELECT 1 FROM mysql.vreplication WHERE workflow='%s'", workflow), -1, false)
	require.Nil(t, err)
	require.True(t, len(result.Rows) >= 1)
}

func TestBranchNormalfunction(t *testing.T) {
	workflowName := "branch_test"
	output, err := clusterInstance.VtctlclientProcess.PrepareBranch(workflowName, "branch_source", "branch_target", "", "", "", "", false, "RAND()<0.1", false)
	require.Nil(t, err)
	require.True(t, strings.HasPrefix(output, "successfully"))
	output, err = clusterInstance.VtctlclientProcess.StartBranch(workflowName)
	require.Nil(t, err)
	require.True(t, strings.HasSuffix(output, "successfully."))
	RequireVRplicationExist(t, workflowName)
	defer func() {
		CleanupDatabase(t)
		clusterInstance.VtctlclientProcess.Cleanupbranch(workflowName)
	}()
}

func TestPrepareBranch(t *testing.T) {
	//vtctlclient --server localhost:15999 Branch -- --source_database branch_source --target_database branch_target --skip_copy_phase=false --workflow_name branch_test --default_filter_rules "RAND()<0.1" Prepare
	output, err := clusterInstance.VtctlclientProcess.PrepareBranch("branch_test", "branch_source", "branch_target", "", "", "", "", false, "RAND()<0.1", false)
	require.Nil(t, err)
	require.True(t, strings.HasPrefix(output, "successfully"))
	defer CleanupDatabase(t)
}
