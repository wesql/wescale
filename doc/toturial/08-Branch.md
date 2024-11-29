---
title: Branch
---

# Branch

## Overview

The Branch feature in WeSQL allows you to copy a database to current Wescale cluster or another Wescale cluster. This is particularly useful when you want to create a development database that mirrors your production environment. This guide explains how to use the Branch feature, including its commands and options.

## General Syntax

The `Branch` command is an umbrella command with several subcommands (`actions`) that define specific operations within the workflow. Each action has its own options to control its behavior.

```shell
Branch -- <options> <action>

action := [Prepare | Start | Stop | SchemaDiff | PrepareMergeBack | StartMergeBack | Cleanup ]
```

## Actions Overview

- **Prepare**: Initializes the branch workflow by copying the database schema from the source to the target database.
- **Start**: Begins data streaming from the source to the target database.
- **Stop**: Halts the data streaming process.
- **SchemaDiff**: Displays differences between the source and target database schemas.
- **PrepareMergeBack**: Prepares to merge schema changes from the target back to the source database.
- **StartMergeBack**: Executes the merge of schema changes.
- **Cleanup**: Cleans up the branch workflow

For a simple database copy, focus on the `Prepare`, `PrepareMergeBack`, `StartMergeBack`, and `Cleanup` commands.

## Step1: Example Scenario Setup

### Create Sample Tables
We'll use a sample environment to demonstrate the Branch feature. Execute the following commands to create the source database and tables:

```sql
CREATE DATABASE IF NOT EXISTS branch_source;

USE branch_source;

CREATE TABLE IF NOT EXISTS branch_source.user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
  ) ENGINE=InnoDB;
  
CREATE TABLE IF NOT EXISTS branch_source.customer (
    customer_id BIGINT NOT NULL AUTO_INCREMENT,
    email VARCHAR(128),
    PRIMARY KEY(customer_id)
  ) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS branch_source.product (
    sku VARCHAR(128),
    description VARCHAR(128),
    price BIGINT,
    PRIMARY KEY(sku)
  ) ENGINE=InnoDB;
  
CREATE TABLE IF NOT EXISTS branch_source.corder (
    order_id BIGINT NOT NULL AUTO_INCREMENT,
    customer_id BIGINT,
    sku VARCHAR(128),
    price BIGINT,
    PRIMARY KEY(order_id)
  ) ENGINE=InnoDB;
```

### Insert Sample Data

You can then insert some sample data into the tables:
```sql
-- Insert data into user table (500 rows)
INSERT INTO branch_source.user (name)
SELECT CONCAT('user_', n)
FROM (
    SELECT ROW_NUMBER() OVER () AS n
    FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) a
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) b
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) c
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
) AS numbers
WHERE n <= 500;

-- Insert data into customer table (200 rows), linking to user table
INSERT INTO branch_source.customer (email, customer_id)
SELECT CONCAT('customer_', n, '@example.com'), n
FROM (
    SELECT ROW_NUMBER() OVER () AS n
    FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) a
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) b
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) c
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
) AS numbers
WHERE n <= 200;

-- Insert data into product table (1000 rows)
INSERT INTO branch_source.product (sku, description, price)
SELECT CONCAT('sku_', n), CONCAT('product_', n), FLOOR(RAND() * 1000 + 1) -- Random price between 1 and 1000
FROM (
    SELECT ROW_NUMBER() OVER () AS n
    FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) a
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) b
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) c
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) e
) AS numbers
WHERE n <= 1000;

-- Insert data into corder table (10000 rows), linking to customer and product tables
INSERT INTO branch_source.corder (customer_id, sku, price)
SELECT c.customer_id, p.sku, p.price
FROM (
    SELECT ROW_NUMBER() OVER () AS n, customer_id
    FROM branch_source.customer
    ORDER BY RAND() -- Randomly select customers
) c
JOIN (
    SELECT ROW_NUMBER() OVER () AS n, sku, price
    FROM branch_source.product
    ORDER BY RAND() -- Randomly select products
) p ON (c.n % 1000) + 1 = p.n  -- Ensure every product is ordered
CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) a
CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) b
WHERE c.n <= 10000;
```

With the tables and data in place, you can now proceed with the Branch feature, or try out the sample queries provided in the [Example Business Queries](#example-business-queries) section.

### Prepare Branch Command Client

We need to use `vtctlclient` to execute the `Branch` command.

If you are using `kubernetes`, you can enter the `wesql-vtcontroller` Pod to run the `vtctlclient` command:
```shell
$ kubectl get po
NAME                           READY   STATUS    RESTARTS      AGE
mycluster-wesql-0-0            2/2     Running   0             25m
mycluster-wesql-1-0            1/1     Running   1 (22m ago)   25m
mycluster-wesql-2-0            1/1     Running   1 (22m ago)   25m
wesql-vtcontroller-0           2/2     Running   0             25m
wesql-vtgate-87f69955c-h62nx   1/1     Running   0             25m
wesql-vtgate-87f69955c-t7lbg   1/1     Running   0             25m

$ kubectl exec -it wesql-vtcontroller-0 -c vtctld -- sh
/vt #    vtctlclient -v
WeScale version: 0.3.7 (git revision cb93f4e1dacce0882dde6ec27ee409c011684034 branch 'HEAD') built on Tue Oct 22 03:40:00 UTC 2024 by root@buildkitsandbox using go1.20.2 linux/amd64
```

If you are using `docker`, you can enter the `wescale` container to run the `vtctlclient` command:
```shell
ubuntu $ docker exec -it wescale bash
[root@3c089b5e14fc wesql-server]$
[root@3c089b5e14fc wesql-server]$   vtctlclient -v
WeScale version: 0.3.7 (git revision cb93f4e1dacce0882dde6ec27ee409c011684034 branch 'HEAD') built on Tue Oct 22 03:40:00 UTC 2024 by root@buildkitsandbox using go1.20.2 linux/amd64
```

## Step2: Create a Branch Workflow

The `Prepare` action create a branch workflow by copying the schema from the source database to the target database.


### Example
Let's copy the schema from the `branch_source` database to the `branch_target` database.

`skip_copy_phase=false` and `stop_after_copy=false` means that we'll copy both schema and data.


```shell
vtctlclient --server localhost:15999 Branch -- \
  --source_database branch_source \
  --target_database branch_target \
  --workflow_name branch_test \
  --skip_copy_phase=false \
  --stop_after_copy=false \
Prepare
```

**Output:**

```
successfully create branch workflow : branch_test sourceDatabase : branch_source targetDatabase : branch_target
rules : 
[source_table:"corder" target_table:"corder" filtering_rule:"select * from corder " create_ddl:"copy" merge_ddl:"copy"]
[source_table:"customer" target_table:"customer" filtering_rule:"select * from customer " create_ddl:"copy" merge_ddl:"copy"]
[source_table:"product" target_table:"product" filtering_rule:"select * from product " create_ddl:"copy" merge_ddl:"copy"]
[source_table:"user" target_table:"user" filtering_rule:"select * from `user` " create_ddl:"copy" merge_ddl:"copy"]
```

The output shows that the branch workflow `branch_test` has been created. Now you can verify that the `branch_target` database has been created:
```sql
mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| branch_source      |
| branch_target      |
| information_schema |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
6 rows in set (0.00 sec)
```


### Optional Parameters
<details>
<summary>Optional Parameters</summary>

For Advanced usage, you can use the following syntax:
```shell
Branch -- \
  --source_database=<source_database> \
  --target_database=<target_database> \
  --workflow_name=<workflow_name> \
  [--source_topo_url=<source_topo_url>] \
  [--tablet_types=<tablet_types>] \
  [--cells=<cells>] \
  [--include=<tables>] \
  [--exclude=<tables>] \
  [--skip_copy_phase=<true|false>] \
  [--stop_after_copy=<true|false>] \
  [--default_filter_rules=<filter_rule>] \
Prepare
```

**Options**

- `--source_database`: Name of the source database.
- `--target_database`: Name of the target database (created if it doesn't exist).
- `--workflow_name`: Identifier for the branch workflow.
- `--source_topo_url`: URL of the source cluster's topology server (defaults to local).
- `--tablet_types`: Types of tablets to use as data sources (e.g., `REPLICA`, `PRIMARY`).
- `--include`: Tables to include from the source database.
- `--exclude`: Tables to exclude from the source database.
- `--skip_copy_phase`: If `true`, only copies the schema without data.
- `--stop_after_copy`: If `true`, stops data synchronization after the initial copy.
- `--default_filter_rules`: Conditions to filter data during copying.

</details>

## Step3: Data Streaming and Transformation

Once `Prepare` command is executed, you can define data transformation rules for each table in the `mysql.branch_table_rules` table.
This allows you to filter, transform, or generate mock data before streaming it to the target database.
All you need to do is update the `filtering_rule` column in the `branch_table_rules` table.

### Example

```sql
UPDATE mysql.branch_table_rules 
SET filtering_rule='SELECT id, gofakeit_generate(''{firstname}:###:???:{moviename}'') AS name FROM user' 
WHERE source_table_name = 'user' and workflow_name='branch_test';

UPDATE mysql.branch_table_rules 
SET filtering_rule='SELECT customer_id, gofakeit_bytype(''regex'', ''^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'') AS email FROM customer' 
WHERE source_table_name = 'customer' and workflow_name='branch_test';
```


## Step4: Start Data Streaming

Use the `Start` action to begin data streaming between the source and target databases. The target database applies ETL operations based on the defined `filtering_rule`.

### Example

```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name=branch_test Start
```

**Output:**
```
Start workflow: branch_test successfully.
```

Then you can insert some data into the source database and verify that it's copied to the target database.
You can see that the data is transformed according to the rules defined in the `branch_table_rules` table.

Since `gofakeit_generate(''{firstname}:###:???:{moviename}'') AS name` is used for the `user` table, the `name` column is populated with random names.
It's very convenient for generating mock data in the target branch for testing purposes.
> For more information on `gofakeit` functions, see the [Using `gofakeit` Functions in Data Streaming and Transformation](#using-gofakeit-functions-in-data-streaming-and-transformation) section.
```sql
mysql> select * from branch_source.user;

mysql> select * from branch_target.user;

mysql> select * from branch_source.customer;

mysql> select * from branch_target.customer;
```

<details>
<summary>Syntax</summary>
Branch -- --workflow_name=${workflow_name} Start
</details>


## Step5: Stop Data Streaming

The `Stop` action halts the data streaming process. Restart it later using the `Start` action if needed.
### Example

```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Stop
```

**Output:**
```
Stop workflow: branch_test successfully.
```

**Note:** If `stop_after_copy` was set to `true` during the `Prepare` action and the data copy is complete, the `Stop` command will have no effect.


<details>
<summary>Syntax</summary>
Branch -- --workflow_name=${workflow_name} Stop
</details>


## Step6: View Schema Differences

Use the `SchemaDiff` action to display differences between the source and target database schemas. This helps decide whether to merge schema changes back into the source database.

### Example

Assuming you've made schema changes to the target database:

```sql
mysql> # Since OnlineDDL is enabled, it return the UUID of the OnlineDDL Job. OnlineDDL runs asynchronously, so you may need to wait for it to complete.
mysql> ALTER TABLE branch_target.product ADD COLUMN v2 INT;
+--------------------------------------+
| uuid                                 |
+--------------------------------------+
| 63ed2d8f_7c09_11ef_82e1_0aa7bf255933 |
+--------------------------------------+
1 row in set (0.02 sec)


mysql> # Since OnlineDDL is enabled, it return the UUID of the OnlineDDL Job. OnlineDDL runs asynchronously, so you may need to wait for it to complete.
mysql> ALTER TABLE branch_target.product ADD COLUMN v3 INT;
+--------------------------------------+
| uuid                                 |
+--------------------------------------+
| 67dd9224_7c09_11ef_82e1_0aa7bf255933 |
+--------------------------------------+
1 row in set (0.01 sec)

mysql> # Check the table schema and make sure the OnlineDDL is completed.
mysql> show create table branch_target.product\G
*************************** 1. row ***************************
       Table: product
Create Table: CREATE TABLE `product` (
  `sku` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `description` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `v2` int DEFAULT NULL,
  `v3` int DEFAULT NULL,
  PRIMARY KEY (`sku`)
) ENGINE=SMARTENGINE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
1 row in set (0.01 sec)
```

Then run the `SchemaDiff` action to compare the source and target database schemas:
```bash
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test SchemaDiff --compare_objects=source_target --output_type=ddl
```

You can see that the target side has two additional columns `v2` and `v3` in the `product` table compared to the source side.
**Output:**
```
The DDLs required to transform from the source schema to the target schema are as follows:
alter table product add column v2 int, add column v3 int
```

<details>
<summary>Syntax</summary>

```shell
Branch -- --workflow_name=${workflow_name} SchemaDiff \
[--compare_objects=<source_target|target_source|source_snapshot|snapshot_source|target_snapshot|snapshot_target>] \
[--output_type=<create_table|ddl|conflict>] \
```
**compare_objects**
(default `source_target`)
- `source`: Source schema, may be changed due to other branches merged.
- `target`: Target schema.
- `snapshot`: Snapshot of source schema when starting branch.

**output_type**
(default `create_table`)
- `create_table`: Show difference of create table SQLs of compare objects.
- `ddl`: Show DDls used to convert from first compare object to second compare object.
- `conflict`: Show whether schema of first compare object can be merged to schema of second compare object without conflicts.

</details>

## Step7: PrepareMergeBack

The `PrepareMergeBack` action is a prerequisite for the `StartMergeBack` action.
It generates DDL statements that will be executed at the source side, so that the schema changes made at the target side can be merged back to the source side.

Since `product` table on the target side has two additional columns `v2` and `v3`, the DDL statement will be generated to add these two columns to the source side.

### Example

```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test PrepareMergeBack --merge_option=override
```

**Output:**
```
Table: product 
DDL: ALTER TABLE `product` ADD COLUMN `v2` int, ADD COLUMN `v3` int
PrepareMergeBack (mergeOption=override) branch_test successfully
```


<details>
<summary>Syntax</summary>

```shell
Branch -- --workflow_name=${workflow_name} PrepareMergeBack \
[--merge_option=<override|diff>] \
```
**merge_option**
(default `override`)
- `override`: Override the source schema with the target schema and output the required DDL.
- `diff`: Merge the target schema into the source schema and output the required DDL. You can use the `SchemaDiff` command to check for conflicts before merging.

</details>



## Step8: StartMergeBack

Use the `StartMergeBack` action to execute the DDL statements generated during `PrepareMergeBack`, applying schema changes to the source database using an online strategy.

### Example

```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test StartMergeBack
```

**Output:**
```
StartMergeBack for 'branch_test' completed successfully. UUIDs:
[c057f330_b1e0_11ee_b7b2_5ea977d56bb7]
```

Monitor the progress of the Online DDL using the provided UUID:

```sql
mysql> SHOW SCHEMA_MIGRATION LIKE 'c057f330_b1e0_11ee_b7b2_5ea977d56bb7'\G
```

When the Online DDL task is complete, the schema of source table should be the same as that of target table.

```sql
mysql> desc branch_source.product;
+-------------+--------------+------+-----+---------+-------+
| Field       | Type         | Null | Key | Default | Extra |
+-------------+--------------+------+-----+---------+-------+
| sku         | varchar(128) | NO   | PRI | NULL    |       |
| description | varchar(128) | YES  |     | NULL    |       |
| price       | bigint       | YES  |     | NULL    |       |
| v2          | int          | YES  |     | NULL    |       |
| v3          | int          | YES  |     | NULL    |       |
+-------------+--------------+------+-----+---------+-------+
5 rows in set (0.005 sec)
```

<details>
<summary>Syntax</summary>
Branch -- --workflow_name=${workflow_name} StartMergeBack
</details>


## Step9: Cleanup a Branch Workflow

The `Cleanup` action removes the metadata of branch workflow. The target database **will not** be deleted.


### Example

```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Cleanup
```

**Output:**
```
Cleanup for workflow 'branch_test' completed successfully.
```

<details>
<summary>Syntax</summary>
Branch -- --workflow_name=${workflow_name} Cleanup
</details>

## Appendix

### Using `gofakeit` Functions in Data Streaming and Transformation

The Branch feature supports `gofakeit_generate` and `gofakeit_bytype` functions for data generation, compatible with MySQL data types.
It only works with the `Branch` feature and is not available for general queries.

**gofakeit_generate:**

Generates random strings based on patterns. Refer to the [gofakeit documentation](https://github.com/brianvoe/gofakeit) for pattern syntax.

**Example:**

```sql
SELECT gofakeit_generate('{firstname} {lastname}') AS full_name;
```

**gofakeit_bytype:**

Generates random data based on specified types.

#### Supported Types and Usage

| Type         | Description                                        | Example Usage                               |
|--------------|----------------------------------------------------|---------------------------------------------|
| `tinyint`    | Generates a random tiny integer.                   | `gofakeit_bytype('tinyint')`                |
| `smallint`   | Generates a random small integer.                  | `gofakeit_bytype('smallint')`               |
| `mediumint`  | Generates a random medium integer.                 | `gofakeit_bytype('mediumint')`              |
| `int`        | Generates a random integer.                        | `gofakeit_bytype('int')`                    |
| `integer`    | Alias for `int`.                                   | `gofakeit_bytype('integer')`                |
| `bigint`     | Generates a random big integer.                    | `gofakeit_bytype('bigint')`                 |
| `float`      | Generates a random float.                          | `gofakeit_bytype('float')`                  |
| `double`     | Generates a random double.                         | `gofakeit_bytype('double')`                 |
| `decimal`    | Generates a random decimal.                        | `gofakeit_bytype('decimal', 5, 2)`          |
| `date`       | Generates a random date.                           | `gofakeit_bytype('date')`                   |
| `datetime`   | Generates a random datetime.                       | `gofakeit_bytype('datetime')`               |
| `timestamp`  | Generates a random timestamp.                      | `gofakeit_bytype('timestamp')`              |
| `time`       | Generates a random time.                           | `gofakeit_bytype('time')`                   |
| `year`       | Generates a random year.                           | `gofakeit_bytype('year')`                   |
| `floatrange` | Generates a random float within a specified range. | `gofakeit_bytype('floatrange', 1.5, 10.0)`  |
| `intrange`   | Generates a random integer within a range.         | `gofakeit_bytype('intrange', 100, 200)`     |
| `name`       | Generates a random name.                           | `gofakeit_bytype('name')`                   |
| `address`    | Generates a random address.                        | `gofakeit_bytype('address')`                |
| `uuid`       | Generates a random UUID.                           | `gofakeit_bytype('uuid')`                   |
| `regex`      | Generates a string matching a regex pattern.       | `gofakeit_bytype('regex', '[A-Z]{5}')`      |

**Example:**

```sql
SELECT gofakeit_bytype('intrange', 1, 100) AS random_number;
```

----

### Example Business Queries

Here are some example queries that demonstrate typical business use cases for the given tables, along with explanations:

**1. Find the top 5 customers by total order value:**

```sql
SELECT c.customer_id, c.email, SUM(co.price) AS total_spent
FROM branch_source.customer c
JOIN branch_source.corder co ON c.customer_id = co.customer_id
GROUP BY c.customer_id, c.email
ORDER BY total_spent DESC
LIMIT 5;
```

**2. Find the most popular product (most frequently ordered):**

```sql
SELECT p.sku, p.description, COUNT(co.order_id) AS order_count
FROM branch_source.product p
JOIN branch_source.corder co ON p.sku = co.sku
GROUP BY p.sku, p.description
ORDER BY order_count DESC
LIMIT 1;
```

**3. Find all orders made by a specific customer:**

```sql
SELECT co.order_id, co.sku, co.price
FROM branch_source.corder co
WHERE co.customer_id = 123; -- Replace 123 with the desired customer ID
```

**4. Find the average order value:**

```sql
SELECT AVG(price) AS average_order_value
FROM branch_source.corder;
```

**5. Total revenue for the most popular product:**

```sql
SELECT SUM(co.price) AS total_revenue
FROM branch_source.corder co
WHERE co.sku = (SELECT sku FROM branch_source.corder GROUP BY sku ORDER BY COUNT(*) DESC LIMIT 1);
```
