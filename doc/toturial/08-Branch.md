# Branch

## Overview

The Branch feature in WeSQL allows you to copy a database from one Wescale cluster (the **source**) to another Wescale cluster (the **target**). This functionality is particularly useful for creating a development database that mirrors your production environment. This guide provides clear instructions on how to use the Branch feature, including its commands and options.

**Source** refers to the Wescale cluster from which schema is copied, while **Target** refers to the Wescale cluster where the schema will be copied to.

---

## General Syntax

The general syntax for Branch commands is as follows:

```
Branch <Action> [with ('key1'='value1', 'key2'='value2', 'key3'='value3');]
```

Where the **Action** can be one of the following:
- **create**
- **diff**
- **prepare_merge_back**
- **merge_back**
- **show**
- **delete**

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

Each branch corresponds to a MySQL instance. Therefore, you need two Wescale clusters: one acting as the source and the other as the target. You can start the two Wescale clusters with the following commands. If you already have a Wescale cluster running locally, you can just start a new one.

Here, we will use the Wescale cluster on port **15306** as the source and another on port **15307** as the target.

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

After connecting to the source Wescale, run the following commands:

```shell
docker exec -it wescale mysql -h127.0.0.1 -P15306
```

Create the following databases and tables:

```sql
DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP DATABASE IF EXISTS test_db3;
CREATE DATABASE test_db1;
CREATE DATABASE test_db2;

CREATE TABLE test_db1.users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_db2.source_orders (
  order_id INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(100) NOT NULL,
  order_date DATE NOT NULL,
  total_amount DECIMAL(10,2),
  status VARCHAR(20)
);
```

Next, connect to the target Wescale:

```shell
docker exec -it wescale15307 mysql -h127.0.0.1 -P15307
```

Create the following databases and tables:

```sql
DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP DATABASE IF EXISTS test_db3;
CREATE DATABASE test_db2;
CREATE DATABASE test_db3;

CREATE TABLE test_db2.target_orders (
  order_id INT PRIMARY KEY AUTO_INCREMENT,
  customer_name VARCHAR(100) NOT NULL
);

CREATE TABLE test_db3.products (
  product_id INT PRIMARY KEY AUTO_INCREMENT,
  product_name VARCHAR(200) NOT NULL,
  price DECIMAL(10,2),
  stock_quantity INT,
  category VARCHAR(50),
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

---

## Basic Usage

In the process of software development, we often need to modify the existing database schema. However, testing directly in the production environment poses significant security risks. Thus, it's common to create a testing environment where schema modifications can be safely tested before being applied back to the production environment. Here, we refer to the production environment as the "source" and the testing environment as the "target."

As described above, a typical workflow involves three major processes: copying schema from source to target, schema modification in target, and merging target schema back to source.

### Schema Copy

After connecting to the target Wescale, check the available databases before copying:

```sql
SHOW DATABASES;
```

To copy the database schema from the source to the target, create a branch with the following command:

```sql
Branch create with (
    'source_host'='wescale',
    'source_port'='15306',
    'source_user'='root',
    'source_password'='passwd'
);
```

*Note: For detailed parameter explanations, refer to the command parameters section later in this document.*

To view the current branch metadata, use:

```sql
Branch show;
```

You'll see the branch status as "created".

And now, check the available databases again, you will find the source schema has been copied to target.

### Schema modification

We suppose we need to modify the `test_db1.users` table, in the target, you could run:

```sql
ALTER TABLE test_db1.users ADD COLUMN new_col INT;
```

### Schema Merge

The Branch functionality in WeSQL allows you to merge the target's schema back to the source. The merging process we provide is called "override," which means that the target schema will overwrite the source schema.

To see the DDLs required to update the source schema to match the target, use:

```sql
Branch prepare_merge_back;
```

If everything is prepared, the branch status will update to "prepared," indicating readiness to merge.

Execute the merge with:

```sql
Branch merge_back;
```

After merging, the schema in the source should reflect the same structure as in the target.

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
