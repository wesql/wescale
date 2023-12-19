/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package branch

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vt/sqlparser"

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
    							weight float,
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

// VtgateExecQuery runs a query on VTGate using given query params
func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

func RequireVRplicationExist(t *testing.T, workflow string) {
	result, err := mysqlConn.ExecuteFetch(fmt.Sprintf("SELECT 1 FROM mysql.vreplication WHERE workflow='%s'", workflow), -1, false)
	require.Nil(t, err)
	require.True(t, len(result.Rows) >= 1)
}

func checkStateOfVreplication(t *testing.T, uuid, expectState string) {
	query, err := sqlparser.ParseAndBind("select state from mysql.vreplication where workflow=%a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	rs := onlineddl.VtgateExecQuery(t, &vtParams, query, "")
	require.NotNil(t, rs)

	require.Equal(t, 1, len(rs.Named().Rows))
	require.Equal(t, expectState, rs.Named().Rows[0].AsString("state", ""))
}

func WaitForVreplicationState(t *testing.T, vtParams *mysql.ConnParams, workflow string, timeout time.Duration, expectStates ...string) string {
	query, err := sqlparser.ParseAndBind("select state from mysql.vreplication where workflow=%a",
		sqltypes.StringBindVariable(workflow),
	)
	require.NoError(t, err)

	statesMap := map[string]bool{}
	for _, state := range expectStates {
		statesMap[string(state)] = true
	}
	startTime := time.Now()
	lastKnownVreplicationState := ""
	for time.Since(startTime) < timeout {
		r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			lastKnownVreplicationState = row["state"].ToString()

			if statesMap[lastKnownVreplicationState] {
				return lastKnownVreplicationState
			}
		}
		time.Sleep(1 * time.Second)
	}
	return lastKnownVreplicationState
}

func TestBranchGoFakeitFunction(t *testing.T) {
	workflowName := "branch_test"
	t.Run("prepare branch", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.PrepareBranch(workflowName, "branch_source", "branch_target", "", "", "", "", false, "RAND()<0.1", false)
		require.Nil(t, err)
		require.True(t, strings.HasPrefix(output, "successfully"))
	})
	t.Run("update filterling rules", func(t *testing.T) {
		VtgateExecQuery(t, &vtParams, `update mysql.branch_table_rules set filtering_rule='select id, gofakeit_generate(\'{firstname}:###:???:{moviename}\') as name from user WHERE id<=100' where source_table_name = 'user';`, "")
		VtgateExecQuery(t, &vtParams, `update mysql.branch_table_rules set filtering_rule='select customer_id, gofakeit_bytype(\'regex\',\'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$\') as email from customer WHERE customer_id<=100' where source_table_name = 'customer';`, "")
		VtgateExecQuery(t, &vtParams, `update mysql.branch_table_rules set filtering_rule='select sku,description,gofakeit_bytype(\'intrange\',110,150) as price,gofakeit_bytype(\'floatrange\',23.5,23.9) as weight from product' where source_table_name = 'product';`, "")
		VtgateExecQuery(t, &vtParams, `update mysql.branch_table_rules set filtering_rule='SELECT order_id,gofakeit_bytype(\'bigint\') as customer_id,gofakeit_generate(\'{firstname}:###:???:{moviename}\') as sku,gofakeit_bytype(\'bigint\') as price FROM corder where customer_id<=100' where source_table_name = 'corder';`, "")
	})
	t.Run("start branch", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.StartBranch(workflowName)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(output, "successfully."))
		RequireVRplicationExist(t, workflowName)
		WaitForVreplicationState(t, &vtParams, workflowName, 5*time.Second, "Stopped")

		// id<=100
		qr := VtgateExecQuery(t, &vtParams, `select max(id) as id from branch_target.user`, "")
		maxID, err := qr.Rows[0][0].ToInt64()
		require.Nil(t, err)
		require.True(t, maxID <= 100)
		// customer_id <= 100
		qr = VtgateExecQuery(t, &vtParams, `select max(customer_id) as id from branch_target.customer`, "")
		customerID, err := qr.Rows[0][0].ToInt64()
		require.Nil(t, err)
		require.True(t, customerID <= 100)

		qr = VtgateExecQuery(t, &vtParams, `select price,weight from branch_target.product limit 100`, "")
		for _, row := range qr.Rows {
			price, err := row[0].ToInt64()
			require.Nil(t, err)
			require.True(t, price >= 110 && price <= 150)
			weight, err := row[1].ToFloat64()
			require.Nil(t, err)
			require.True(t, weight >= 23.5 && weight <= 23.9)
		}
	})
	defer func() {
		CleanupDatabase(t)
		clusterInstance.VtctlclientProcess.Cleanupbranch(workflowName)
	}()
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
