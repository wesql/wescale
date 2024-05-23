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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/wesql/wescale/go/test/endtoend/cluster"
	"github.com/wesql/wescale/go/vt/schema"

	"github.com/stretchr/testify/assert"

	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/test/endtoend/onlineddl"
	"github.com/wesql/wescale/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	"github.com/wesql/wescale/go/mysql"
)

type BranchStatus string

const (
	BranchStateOfPrepare   = "Prepare"
	BranchStateOfRunning   = "Running"
	BranchStateOfStop      = "Stop"
	BranchStateOfCompleted = "Completed"
	BranchStateOfError     = "Error"
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

func ExtractUUIDs(output string) []string {
	uuidPattern := regexp.MustCompile(`\[([a-fA-F0-9_]+)\]`)
	extractedUUIDs := []string{}

	matches := uuidPattern.FindAllStringSubmatch(output, -1)
	for _, match := range matches {
		if len(match) == 2 {
			extractedUUIDs = append(extractedUUIDs, match[1])
		}
	}

	return extractedUUIDs
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
	query, err := sqlparser.ParseAndBind("select * from mysql.vreplication where workflow=%a",
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
			t.Logf("message : %v", row["message"].ToString())
			if statesMap[lastKnownVreplicationState] {
				return lastKnownVreplicationState
			}
		}
		time.Sleep(1 * time.Second)
	}
	return lastKnownVreplicationState
}

// CheckMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func CheckMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)

	count := 0
	for _, row := range r.Named().Rows {
		if row["migration_uuid"].ToString() != uuid {
			continue
		}
		fmt.Printf("uuid %s status is %s\n", uuid, row["migration_status"].ToString())
		for _, expectStatus := range expectStatuses {
			if row["migration_status"].ToString() == string(expectStatus) {
				count++
				break
			}
		}
	}
	assert.Equal(t, len(shards), count)
}

func CheckBranchStatus(t *testing.T, vtParams *mysql.ConnParams, workflow string, expectStatuses ...BranchStatus) {
	query, err := sqlparser.ParseAndBind("select * from mysql.branch_jobs where workflow_name=%a",
		sqltypes.StringBindVariable(workflow),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)

	count := 0
	for _, row := range r.Named().Rows {
		if row["workflow_name"].ToString() != workflow {
			continue
		}
		for _, expectStatus := range expectStatuses {
			if row["status"].ToString() == string(expectStatus) {
				count++
				break
			}
		}
	}
	assert.Equal(t, 1, count)
}

func WaitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard.Name] = true
	}
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		countMatchedShards := 0
		r := VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			shardName := row["shard"].ToString()
			if !shardNames[shardName] {
				// irrelevant shard
				continue
			}
			lastKnownStatus = row["migration_status"].ToString()
			message := row["message"].ToString()
			if lastKnownStatus == string(schema.OnlineDDLStatusFailed) {
				t.Logf("schemaMigration fail, message : %v", message)
			}
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == len(shards) {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
		time.Sleep(1 * time.Second)
	}
	return schema.OnlineDDLStatus(lastKnownStatus)
}

// WaitForBranchStatus waits for a branch to reach either provided statuses (returns immediately), or eventually time out
func WaitForBranchStatus(t *testing.T, vtParams *mysql.ConnParams, workflow string, timeout time.Duration, expectStatuses ...BranchStatus) BranchStatus {
	query, err := sqlparser.ParseAndBind("select * from mysql.branch_jobs where workflow_name=%a",
		sqltypes.StringBindVariable(workflow),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		countMatchedShards := 0
		r := VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			workflowName := row["workflow_name"].ToString()
			if workflow != workflowName {
				// irrelevant shard
				continue
			}
			lastKnownStatus = row["status"].ToString()
			message := row["message"].ToString()
			if lastKnownStatus == BranchStateOfError {
				t.Logf("schemaMigration fail, message : %v", message)
			}
			if row["migration_uuid"].ToString() == workflow && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == 1 {
			return BranchStatus(lastKnownStatus)
		}
		time.Sleep(1 * time.Second)
	}
	return BranchStatus(lastKnownStatus)
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

func TestBranchMergeBackOverride(t *testing.T) {
	workflowName := "TestBranchMergeBackOverride"
	defer func() {
		ctx := context.Background()
		branchSourceParams := mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: "branch_source",
		}
		conn, err := mysql.Connect(ctx, &branchSourceParams)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch("DROP TABLE IF EXISTS news_table;\n", -1, false)
		require.Nil(t, err)
		CleanupDatabase(t)
		clusterInstance.VtctlclientProcess.Cleanupbranch(workflowName)
	}()
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
	t.Run("change target schema", func(t *testing.T) {
		vtParamsTmp := mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: "branch_target",
		}
		VtgateExecQuery(t, &vtParamsTmp, "alter table user add column v3 int", "")
		VtgateExecQuery(t, &vtParamsTmp, "create table new_table(v1 int,v2 int)", "")
	})
	t.Run("PrepareMergeBack", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.BranchPrepareMergeBackOverride(workflowName)
		require.Nil(t, err)
		require.True(t, strings.Contains(output, "successfully"))
		t.Logf("output : %v", output)
		qr := VtgateExecQuery(t, &vtParams, "select 1 from mysql.branch_table_rules where source_table_name='new_table'", "")
		require.Equal(t, len(qr.Rows[0]), 1)
		qr = VtgateExecQuery(t, &vtParams, "select merge_ddl from mysql.branch_table_rules where source_table_name='user'", "")
		alterDDL := qr.Rows[0][0].ToString()
		require.True(t, strings.HasPrefix(alterDDL, "ALTER TABLE"))
		qr = VtgateExecQuery(t, &vtParams, "select merge_ddl from mysql.branch_table_rules where source_table_name='new_table'", "")
		createDDL := qr.Rows[0][0].ToString()
		require.True(t, strings.HasPrefix(createDDL, "CREATE TABLE"))

	})
	t.Run("start merge back", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.BranchStartMergeBack(workflowName)
		require.Nil(t, err)
		require.True(t, strings.Contains(output, "successfully."))
		uuids := ExtractUUIDs(output)
		for _, uuid := range uuids {
			WaitForMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, uuid, 30*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		}
		vtParamsTmp := mysql.ConnParams{
			Host:   clusterInstance.Hostname,
			Port:   clusterInstance.VtgateMySQLPort,
			DbName: "branch_source",
		}
		qr := VtgateExecQuery(t, &vtParamsTmp, "show tables like 'new_table'", "")
		t.Logf("%v", qr.Rows)
		require.Equal(t, len(qr.Rows[0]), 1)
		require.Equal(t, qr.Rows[0][0].ToString(), "new_table")
	})
}

func TestBranchNormalFunction(t *testing.T) {
	workflowName := "TestBranchNormalFunction"
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
	workflowName := "TestPrepareBranch"
	//vtctlclient --server localhost:15999 Branch -- --source_database branch_source --target_database branch_target --skip_copy_phase=false --workflow_name branch_test --default_filter_rules "RAND()<0.1" Prepare
	output, err := clusterInstance.VtctlclientProcess.PrepareBranch(workflowName, "branch_source", "branch_target", "", "", "", "", false, "RAND()<0.1", false)
	require.Nil(t, err)
	require.True(t, strings.HasPrefix(output, "successfully"))
	defer CleanupDatabase(t)
}

func TestBranchWatcher(t *testing.T) {
	workflowName := "TestBranchWatcher"
	defer func() {
		CleanupDatabase(t)
		clusterInstance.VtctlclientProcess.Cleanupbranch(workflowName)
	}()
	t.Run("prepare branch", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.PrepareBranch(workflowName, "branch_source", "branch_target", "", "", "", "", false, "RAND()<0.1", false)
		require.Nil(t, err)
		require.True(t, strings.HasPrefix(output, "successfully"))
		time.Sleep(2 * time.Second)
		CheckBranchStatus(t, &vtParams, workflowName, BranchStateOfPrepare)
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
		clusterInstance.VtctlclientProcess.StopBranch(workflowName)
		time.Sleep(2 * time.Second)
		CheckBranchStatus(t, &vtParams, workflowName, BranchStateOfStop)
		// start again
		output, err = clusterInstance.VtctlclientProcess.StartBranch(workflowName)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(output, "successfully."))
		WaitForVreplicationState(t, &vtParams, workflowName, 5*time.Second, "Stopped")
		time.Sleep(3 * time.Second)
		CheckBranchStatus(t, &vtParams, workflowName, BranchStateOfCompleted)
	})
}

func ExecuteOnlineDDLAndWaitSuccess(t *testing.T, vtParams *mysql.ConnParams, query string) {
	var uuid string
	result := onlineddl.VtgateExecDDL(t, vtParams, "vitess", query, "")
	require.NotNil(t, result)
	row := result.Named().Row()
	require.NotNil(t, row)
	uuid = row.AsString("uuid", "")
	assert.Equal(t, schema.OnlineDDLStatusComplete, onlineddl.WaitForMigrationStatus(t, vtParams, clusterInstance.Keyspaces[0].Shards, uuid, 5*time.Minute, schema.OnlineDDLStatusComplete))
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, dbName, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), dbName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.GreaterOrEqual(t, len(queryResult.Rows[0]), 2) // table name, create statement, and if it's a view then additional columns
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func checkTableColExist(t *testing.T, dbName, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], dbName, tableName)
		fmt.Printf("table create statement is %s\n", createStatement)
		assert.Contains(t, createStatement, expectColumn)
	}
}

func checkTableColNotExist(t *testing.T, dbName, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], dbName, tableName)
		fmt.Printf("table create statement is %s\n", createStatement)
		assert.NotContains(t, createStatement, expectColumn)
	}
}

func TestBranchMergeBackDiff(t *testing.T) {
	workflowName := "TestBranchMergeBackDiff"
	sourceDatabase := "diff_source"
	targetDatabase := "diff_target"

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	vtParamsSource := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: sourceDatabase,
	}
	vtParamsTarget := mysql.ConnParams{
		Host:   clusterInstance.Hostname,
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: targetDatabase,
	}

	// create a new database diff_source as sourceSchema
	t.Run("create source schema", func(t *testing.T) {
		_, err = conn.ExecuteFetch(fmt.Sprintf("create database %s", sourceDatabase), -1, false)
		require.Nil(t, err)

		sourceSchemaStr := `CREATE TABLE foo (
						id INT NOT NULL,
						col1 INT NOT NULL,
						col2 VARCHAR(255) NOT NULL,
						PRIMARY KEY(id),
						KEY col1_index(col1)
				        )`
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsSource, sourceSchemaStr)
		qr := VtgateExecQuery(t, &vtParamsSource, "show tables like 'foo'", "")
		t.Logf("%v", qr.Rows)
		require.Equal(t, len(qr.Rows[0]), 1)
		require.Equal(t, qr.Rows[0][0].ToString(), "foo")
	})

	t.Run("prepare branch", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.PrepareBranch(workflowName, sourceDatabase, targetDatabase, "", "", "", "", false, "", false)
		require.Nil(t, err)
		require.True(t, strings.HasPrefix(output, "successfully"))
	})

	t.Run("start branch", func(t *testing.T) {
		output, err := clusterInstance.VtctlclientProcess.StartBranch(workflowName)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(output, "successfully."))
		RequireVRplicationExist(t, workflowName)
		WaitForVreplicationState(t, &vtParams, workflowName, 5*time.Second, "Stopped")
	})

	t.Run("change schemas: add colSource on sourceSchema, add colTarget on targetSchema, then PrepareMergeBack", func(t *testing.T) {
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsSource, "alter table foo add colSource int")
		checkTableColExist(t, sourceDatabase, "foo", "colSource")
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsTarget, "alter table foo add colBranch int")
		checkTableColExist(t, targetDatabase, "foo", "colBranch")

		// PrepareMergeBack but not start
		output, err := clusterInstance.VtctlclientProcess.BranchPrepareMergeBackDiff(workflowName)
		require.Nil(t, err)
		require.True(t, strings.Contains(output, "conflict"))

		// then drop col we add just now, make source and target schemas equals to snapshot schema
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsSource, "alter table foo drop colSource")
		checkTableColNotExist(t, sourceDatabase, "foo", "colSource")
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsTarget, "alter table foo drop colBranch")
		checkTableColNotExist(t, targetDatabase, "foo", "colBranch")
	})

	t.Run("change schemas: col1 on sourceSchema, add col3 on targetSchema", func(t *testing.T) {
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsSource, "alter table foo drop col1")
		checkTableColNotExist(t, sourceDatabase, "foo", "col1")
		ExecuteOnlineDDLAndWaitSuccess(t, &vtParamsTarget, "alter table foo add col3 int")
		checkTableColExist(t, targetDatabase, "foo", "col3")
	})

	t.Run("merge back and check: merge successfully, source has col3, but doesn't have col1; target has col3 and col1", func(t *testing.T) {
		// Although we has called prepareMergeBack just now, it doesn't matter,
		// because prepareMergeBack is idempotent, it will not keep schema diffs computed before
		output, err := clusterInstance.VtctlclientProcess.BranchPrepareMergeBackDiff(workflowName)
		require.Nil(t, err)
		require.True(t, strings.Contains(output, "successfully"))
		t.Logf("output : %v", output)

		output, err = clusterInstance.VtctlclientProcess.BranchStartMergeBack(workflowName)
		require.Nil(t, err)
		require.True(t, strings.Contains(output, "successfully."))
		uuids := ExtractUUIDs(output)
		for _, uuid := range uuids {
			WaitForMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, uuid, 30*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		}

		// source has col3, but doesn't have col1;
		checkTableColExist(t, sourceDatabase, "foo", "col3")
		checkTableColNotExist(t, sourceDatabase, "foo", "col1")

		// target has col3 and col1
		checkTableColExist(t, targetDatabase, "foo", "col3")
		checkTableColExist(t, targetDatabase, "foo", "col1")
	})

	// we can not merge back a branch again
	t.Run("can not merge back again", func(t *testing.T) {
		// if run start merge back directly, it will not execute nothing,
		// because all need_merge_back has been set as false
		output, err := clusterInstance.VtctlclientProcess.BranchStartMergeBack(workflowName)
		require.Nil(t, err)
		uuids := ExtractUUIDs(output)
		require.Equal(t, int(0), len(uuids))

		// if run prepareMerge back, it will say "has been merged back"
		output, err = clusterInstance.VtctlclientProcess.BranchPrepareMergeBackDiff(workflowName)
		require.Nil(t, err)
		t.Logf("output : %v", output)
		require.True(t, strings.Contains(output, "has been merged back"))

	})

	// clean database and table entries
	t.Run("clean up", func(t *testing.T) {
		_, err = clusterInstance.VtctlclientProcess.Cleanupbranch(workflowName)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch(fmt.Sprintf("drop database if exists %s", sourceDatabase), -1, false)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch(fmt.Sprintf("drop database if exists %s", targetDatabase), -1, false)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch("delete from mysql.vreplication where 1=1", -1, false)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch("delete from mysql.branch_jobs where 1=1", -1, false)
		require.Nil(t, err)
		_, err = conn.ExecuteFetch("delete from mysql.branch_table_rules where 1=1", -1, false)
		require.Nil(t, err)
	})
}
