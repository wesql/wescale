# Branch

## Overview

The Branch feature in WeSQL enables you to create a target database with the same schema as your source database. This approach allows you to safely develop and test new schema changes in isolation, without affecting the production environment. After finalizing and validating your changes in the target database, you can merge these schema modifications back into the source database. This workflow ensures a controlled and reliable process for database schema evolution.

---

## Actions Overview

- **create**: Initializes the branch workflow by copying the database schema from the source to the target database.
- **diff**: Displays differences between the source and target database schemas.
- **prepare_merge_back**: Prepares to merge schema changes from the target back to the source database.
- **merge_back**: Executes the merge of schema changes.
- **show**: Displays the branch metadata.
- **delete**: Deletes the branch metadata.

---

## Prerequisites

### Create Cluster

To use the Branch feature, you need two WeScale clusters: a **source** cluster and a **target** cluster. The source cluster is where your production schema resides, while the target cluster is where you’ll test and refine your changes.

If you already have a WeScale cluster running locally, simply start another one as the target. For example, the commands below start a source cluster on port **15306** and a target cluster on port **15307**:


```shell
docker network create wescale-network

# Source cluster
docker run -itd --network wescale-network --name mysql-server \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_ROOT_HOST=% \
  -e MYSQL_LOG_CONSOLE=true \
  mysql/mysql-server:8.0.32 \
  --bind-address=0.0.0.0 \
  --port=3306 \
  --log-bin=binlog \
  --gtid_mode=ON \
  --enforce_gtid_consistency=ON \
  --log_replica_updates=ON \
  --binlog_format=ROW

docker run -itd --network wescale-network --name wescale \
  -p 15306:15306 \
  -w /vt/examples/wesql-server \
  -e MYSQL_ROOT_USER=root \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_PORT=3306 \
  -e MYSQL_HOST=mysql-server \
  -e CONFIG_PATH=/vt/config/wescale/default \
  apecloud/apecloud-mysql-scale:0.3.8-alpha4 \
  /vt/examples/wesql-server/init_single_node_cluster.sh

# Target cluster
docker run -itd --network wescale-network --name mysql-server3307 \
  -p 3307:3307 \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_ROOT_HOST=% \
  -e MYSQL_LOG_CONSOLE=true \
  mysql/mysql-server:8.0.32 \
  --bind-address=0.0.0.0 \
  --port=3307 \
  --log-bin=binlog \
  --gtid_mode=ON \
  --enforce_gtid_consistency=ON \
  --log_replica_updates=ON \
  --binlog_format=ROW

docker run -itd --network wescale-network --name wescale15307 \
  -p 15307:15307 \
  -w /vt/examples/wesql-server \
  -e MYSQL_ROOT_USER=root \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_PORT=3307 \
  -e MYSQL_HOST=mysql-server3307 \
  -e VTGATE_MYSQL_PORT=15307 \
  -e CONFIG_PATH=/vt/config/wescale/default \
  apecloud/apecloud-mysql-scale:0.3.8-alpha4 \
  /vt/examples/wesql-server/init_single_node_cluster.sh
```

### Initialize Data

Use the following commands to set up initial tables in the source cluster:

```shell
$ docker exec -it wescale mysql -h127.0.0.1 -P15306

DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
CREATE DATABASE test_db1;
CREATE DATABASE test_db2;

CREATE TABLE test_db1.users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_db2.orders (
  order_id INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(100) NOT NULL,
  order_date DATE NOT NULL,
  total_amount DECIMAL(10,2),
  status VARCHAR(20)
);
```

---

## Example Usage

In the process of software development, we often need to modify the existing database schema. However, testing directly in the production environment poses significant security risks. Thus, it's common to create a testing environment where schema modifications can be safely tested before being applied back to the production environment. Here, we refer to the production environment as the "source" and the testing environment as the "target."

As described above, a typical workflow involves three major processes: copying schema from source to target, schema modification in target, and merging target schema back to source.

### Step 1: Branch Create

Connect to the target WeScale cluster and run the following command to create a branch. 
`Branch create` command will copy the schema from the source to the target, and establish a branch relationship between the two clusters.

```sql
$ docker exec -it wescale15307 mysql -h127.0.0.1 -P15307

mysql> Branch create with (
    'source_host'='wescale',
    'source_port'='15306',
    'source_user'='root',
    'source_password'='passwd'
);
Query OK, 0 rows affected (0.214 sec)
```

To view the branch metadata, use:

```sql
MySQL [(none)]> Branch show;
+-----------+---------+-------------+-------------+-------------+---------+--------------------------------------------------------------------------+
| name      | status  | source host | source port | source user | include | exclude                                                                  |
+-----------+---------+-------------+-------------+-------------+---------+--------------------------------------------------------------------------+
| my_branch | created | wescale     | 15306       | root        | *       | information_schema,mysql.performance_schema,sys,mysql,performance_schema |
+-----------+---------+-------------+-------------+-------------+---------+--------------------------------------------------------------------------+
1 row in set (0.010 sec)
```

You'll see the branch status as "created".
And now, check the databases in the `target` side, you will find the source database schema has been copied to target side.

```sql
MySQL [(none)]> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| mysql              |
| performance_schema |
| sys                |
| test_db1           |
| test_db2           |
+--------------------+
6 rows in set (0.001 sec)
```

### Step 2: Perform Your Schema Modifications on the Target

Once the branch is created and the schema is copied over to the target, you can safely experiment with changes. For example, you might add new columns and indexes or create additional tables. These changes will not affect the source until you explicitly merge them back.

```sql
ALTER TABLE test_db1.users ADD COLUMN phone VARCHAR(20);

ALTER TABLE test_db1.users ADD INDEX idx_phone (phone);

ALTER TABLE test_db2.orders ADD COLUMN payment_type VARCHAR(20);

ALTER TABLE test_db2.orders ADD INDEX idx_date_status (order_date, status);

CREATE TABLE test_db2.products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'on_sale',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Step 3: Branch Diff

Use the `Branch diff` command to compare the target and source schemas, and generate the DDL statements needed to align the source with the target. This lets you preview the changes before applying them back to the source environment.

```sql
mysql> Branch diff\G
```

### Step 4: Branch Prepare Merge Back

The `Branch prepare_merge_back` command persists the DDL statements identified by `Branch diff` into the branch’s metadata. This step helps ensure that all schema changes can be cleanly merged back into the source cluster.

```sql
Branch prepare_merge_back;
```

### Step 5: Branch Merge Back

After preparing the merge, you can apply the changes to the source cluster by running:

```sql
Branch merge_back;
```

This will execute the stored DDL statements on the source cluster, bringing it up to date with the target schema. At this point, your production schema now includes all the tested modifications you performed in the target environment.

---

## Advanced

### Idempotency

Branch commands are idempotent. This means if a command fails, simply re-executing the command will continue from where it left off.

For instance, the `Branch create` command involves two main phases: retrieving the schema from the source and applying it to the target. If a crash occurs while fetching the schema, just re-execute the `Branch create` command without additional operations.

> It is important to note that the idempotency of the `branch merge_back` command is not yet perfect. Each time branch merge_back is executed, it begins by sequentially executing the "unmerged" DDLs generated by the `branch prepare_merge_back `command (viewable with the branch show command) and marks them as "merged" once executed. **Due to potential crashes, there might be DDLs that are executed but not marked as merged. Checking these DDLs is a task we plan to address in the future.**

### Command Parameter Explanation

| Command Action         | Parameter               | Description                                                                                                                                                                                                                                                                                                                                                                        | Default Value | Required |
|------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|----------|
| `Branch create`        | `name`                  | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |
|                        | `source_host`           | Host of the source database                                                                                                                                                                                                                                                                                                                                                        |              | Yes      |
|                        | `source_port`           | Port of the source database                                                                                                                                                                                                                                                                                                                                                        | 3306         | No       |
|                        | `source_user`           | Username for the source database                                                                                                                                                                                                                                                                                                                                                   | root         | No       |
|                        | `source_password`       | Password for the source database                                                                                                                                                                                                                                                                                                                                                   |              | No       |
|                        | `include_databases`     | Whitelist of databases to process                                                                                                                                                                                                                                                                                                                                                  | *            | No       |
|                        | `exclude_databases`     | Blacklist of databases to process. system databases like information_schema, mysql, performance_schema and sys are alwats excluded.                                                                                                                                                                                                                                                |              | No       |
| `Branch diff`          | `name`                  | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |
|                        | `compare_objects`       | Comparison objects. Should be one of `source_target`, `target_source`, `source_snapshot`, `snapshot_source`, `target_snapshot`, `snapshot_target`, `source_target`. <br/>`source` refers to the real-time schema of the source. <br/>`target` refers to the real-time schema of the target.<br/> `snapshot` refers to the schema of the source at the time the branch was created. | target_source | No       |
| `Branch prepare_merge_back` | `name`           | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |
| `Branch merge_back`    | `name`                  | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |
| `Branch show`          | `name`                  | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |
|                        | `show_option`           | Options for display. Should be one of `status`, `snapshot`, `merge_back_ddl`.                                                                                                                                                                                                                                                                                                      | status        | No       |
| `Branch delete`        | `name`                  | Name of the branch                                                                                                                                                                                                                                                                                                                                                                 | origin    | No       |

### State Transitions

Branch can have the following states:

- **Init**: Initial state after creating the branch.
- **Fetched**: Snapshot saved to the target.
- **Created**: Snapshot has been applied to the target.
- **Preparing**: Generating the merge back DDL.
- **Prepared**: DDL generated and saved, ready for merging.
- **Merging**: Applying DDL to the source.
- **Merged**: All DDLs have been applied to the source successfully.

![BranchStatus](images/BranchStatus.png) 
