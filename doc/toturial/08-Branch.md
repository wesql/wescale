---
title: Branch
---

# Background
The Branch feature is used to copy a database to another Wescale cluster or a local cluster. In a production environment, you can use Branch to create a database identical to the one in the development environment. This document will explain how to use the Branch function.`Branch` is an "umbrella" command. The `action` sub-command defines the operation on the workflow. Each `action` can have their own `options`.

# General Syntax of Branch Command
The Branch operation is executed by the vtctld server, which maintains a gRPC connection with vttablets. Essentially, vtctld acts like a commander.

Note : If you want to use the Branch function, **please ensure that the vtctld server on the target end is open.**
```
Branch -- <options> <action>

action := [Prepare | Start | Stop | PrepareMergeBack | StartMergeBack | Cleanup ｜ Schemadiff]
-- Each action has its own options that can be used to control its behavior.
```

# Actions
Branch commands contain Prepare,Start,Stop,PrepareMergeBack,StartMergeBack and SchemaDiff command.

If your goal is simply to copy a database, please focus on the 'Prepare', 'Start', and 'Stop' commands. These commands can be used to complete the database copy workflow

The 'PrepareMergeBack' and 'StartMergeBack' commands are typically used to merge the target database schema from the target database into the source database.

Before the tutorial of Branch, We will use this environment as our primary example throughout this tutorial. Execute the following SQL commands to create the database and tables:
```sql
mysql -h127.0.0.1 -P15306 -e 'create database if not exists branch_source'

mysql -h127.0.0.1 -P15306 -e 'create table if not exists branch_source.product(
                                sku varchar(128),
                                description varchar(128),
                                price bigint,
                                primary key(sku)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.customer(
                                customer_id bigint not null auto_increment,
                                email varchar(128),
                                primary key(customer_id)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.corder(
                                order_id bigint not null auto_increment,
                                customer_id bigint,
                                sku varchar(128),
                                price bigint,
                                primary key(order_id)
                              ) ENGINE=InnoDB;
                              CREATE TABLE if not exists branch_source.user (
                                  id INT AUTO_INCREMENT PRIMARY KEY auto_increment,
                                  name VARCHAR(255) NOT NULL
                              ) ENGINE=InnoDB;'
```
## Prepare
In the preparation phase, vtctld inserts the job and the corresponding table_rules into mysql.branch_jobs and mysql.branch_table_rules. You can customize a filter by modifying the information in mysql.branch_table_rules.
```
Branch -- 
--source_database=<source_database> 
--target_database=<target_database> 
--workflow_name=<workflow_name> 
[--source_topo_url=<source_topo_url>] 
[--tablet_types=<source_typelet_type>] 
[--cells=<cells>] 
[--include=<tables>]
[--exclude=<tables>]
[--skip_copy_phase =<true/false>]
[--stop_after_copy=<true/false>]
[--default_filter_rules=<filter_rule>]
Prepare
```
+ source_database : Specify the target database name.
+ target_database : Specify the target database name (it will be automatically created if it does not exist).
+ workflow_name   : Specify Branch workflow (Use it to query branch status)
+ source_topo_url : The source cluster's topology server URL (default is the local topology server).
+ tablet_types    : The data source tablet type (e.g., REPLICA, PRIMARY, etc.).
+ include         : Specify tables to include from the source database.
+ exclude         : Specify tables to exclude from the source database.
+ skip_copy_phase :  Only copy the schema, not the data.
+ stop_after_copy : Continuously synchronize data between source and target if false.
+ default_filter_rules : Append conditions to the 'where' clause to filter specific data.
### Usage
```shell
vtctlclient --server localhost:15999 Branch -- --source_database branch_source --target_database branch_target --skip_copy_phase=false  --stop_after_copy=false --workflow_name
 branch_test --default_filter_rules "RAND()<0.1" Prepare

successfully create branch workflow : branch_test sourceDatabase : branch_source targetDatabase : branch_target
rules : 
[source_table:"corder" target_table:"corder" filtering_rule:"select * from corder WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"customer" target_table:"customer" filtering_rule:"select * from customer WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"product" target_table:"product" filtering_rule:"select * from product WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"user" target_table:"user" filtering_rule:"select * from `user` WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
```
## data transformer
Before the data Stream starts, you can perform data transformer by modifying the filtering_rule attribute in the mysql.branch_table_rules table.
We are compatible with the content in [gofakeit](https://github.com/brianvoe/gofakeit). You can use gofakeit_generate to generate a specific string or gofakeit_bytype to generate a specific type.
The filtering rule will be executed on the source MySQL instance, and transformation and filtering will be carried out on the vttablet.
### usage
```sql
update mysql.branch_table_rules set filtering_rule='select id, gofakeit_generate(\'{firstname}:###:???:{moviename}\') as name from user WHERE id<=100' where source_table_name = 'user';
update mysql.branch_table_rules set filtering_rule='select customer_id, gofakeit_bytype(\'regex\',\'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$\') as email from `customer` WHERE customer_id<=100' where source_table_name = 'customer';
update mysql.branch_table_rules set filtering_rule='select sku,description,gofakeit_bytype(\'intrange\',110,150) as price,gofakeit_bytype(\'floatrange\',23.5,23.9) as weight from `product`' where source_table_name = 'product';
update mysql.branch_table_rules set filtering_rule='SELECT order_id,gofakeit_bytype(\'bigint\') as customer_id,gofakeit_generate(\'{firstname}:###:???:{moviename}\') as sku,gofakeit_bytype(\'bigint\') as price FROM corder where customer_id<=100' where source_table_name = 'corder';
```
## Start
`Start` will start a stream between source side and the target side. The target side will perform ETL operations based on table_rules.
Before start a Branch
```
Branch -- --workflow_name=<workflow_name> Start
```
### usage
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Start

Start workflow:branch_test successfully.
```
## Stop
`Stop` will stop the previous stream, and you can use `Start` to restart the stream again
```
Branch -- --workflow_name=<workflow_name> Stop
```
### usage
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Stop

Start workflow branch_test successfully
```
Note : If the branch copy step is complete and stop_after_copy is set to true, the Stop function will become ineffective.
## SchemaDiff
`SchemaDiff` displays the differences between two database schemas. This tool aids in deciding whether to merge changes back into the source schema.
```
Branch -- --workflow_name=<workflow_name> SchemaDiff
```
### usage
```shell
mysql -h127.0.0.1 -P15306 -e 'alter table branch_target.product add column v2 int;'
mysql -h127.0.0.1 -P15306 -e 'alter table branch_target.product add column v3 int;'

vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test schemadiff

table product is diff
branch_target:name:"product" schema:"CREATE TABLE `product` (\n  `sku` varchar(128) NOT NULL,\n  `description` varchar(128) DEFAULT NULL,\n  `price` bigint DEFAULT NULL,\n  `v2` int DEFAULT NULL,\n  `v3` int DEFAULT NULL,\n  PRIMARY KEY (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" primary_key_columns:"sku" type:"BASE TABLE" data_length:16384
branch_source:name:"product" schema:"CREATE TABLE `product` (\n  `sku` varchar(128) NOT NULL,\n  `description` varchar(128) DEFAULT NULL,\n  `price` bigint DEFAULT NULL,\n  PRIMARY KEY (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" primary_key_columns:"sku" type:"BASE TABLE" data_length:1589248 row_count:5294
```
## PrepareMergeBack
`PrepareMergeBack` will construct the differences between the source database and the target database, then generate executable DDL and add it to the branch_table_rules table.
```
Branch -- --workflow_name=<workflow_name> PrepareMergeBack
```
### usage
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test PrepareMergeBack

PrepareMergeBack branch_test successfully 
table: product entry: ALTER TABLE `product` ADD COLUMN `v2` int, ADD COLUMN `v3` int
```
## StartMergeBack
`PrepareMergeBack` will execute the DDLs that are generated in the PrepareMergeBack stage using the 'online' strategy.
```
Branch -- --workflow_name=<workflow_name> StartMergeBack
```
### usage
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test StartMergeBack

Start mergeBack branch_test successfully. uuid list:
[c057f330_b1e0_11ee_b7b2_5ea977d56bb7]
```
After initiating StartMergeBack, we receive a UUID for OnlineDDL. This UUID can then be used with the command `show vitess_migrations like [uuid]` to query the status of the onlineDDL.
```sql
mysql> show vitess_migrations like 'c057f330_b1e0_11ee_b7b2_5ea977d56bb7' \G;
*************************** 1. row ***************************
                             id: 1
                 migration_uuid: c057f330_b1e0_11ee_b7b2_5ea977d56bb7
                       keyspace: branch_source
                          shard: 0
                   mysql_schema: branch_source
                    mysql_table: product
            migration_statement: alter table product add column v2 int, add column v3 int
                       strategy: online
                        options: 
                added_timestamp: 2024-01-13 14:55:35
            requested_timestamp: 2024-01-13 14:55:35
                ready_timestamp: NULL
              started_timestamp: 2024-01-13 14:55:36
             liveness_timestamp: 2024-01-13 14:55:42
            completed_timestamp: 2024-01-13 14:55:43.427318
              cleanup_timestamp: NULL
               migration_status: complete
           status_before_paused: NULL
                       log_path: 
                      artifacts: _vt_HOLD_c354897cb1e011ee9dd55ea977d56bb7_20240114065540,_c057f330_b1e0_11ee_b7b2_5ea977d56bb7_20240113145536_vrepl,
                        retries: 0
                         tablet: zone1-0000000100
                 tablet_failure: 0
                       progress: 100
              migration_context: vtctl:c05515b6-b1e0-11ee-b7b2-5ea977d56bb7
                     ddl_action: alter
                        message: 
                    eta_seconds: 0
                    rows_copied: 5000
                     table_rows: 5000
              added_unique_keys: 0
            removed_unique_keys: 0
                       log_file: 
       retain_artifacts_seconds: 86400
            postpone_completion: 0
       removed_unique_key_names: 
dropped_no_default_column_names: 
          expanded_column_names: 
               revertible_notes: 
               allow_concurrent: 0
                  reverted_uuid: 
                        is_view: 0
              ready_to_complete: 1
                 stowaway_table: 
      vitess_liveness_indicator: 1705128939
            user_throttle_ratio: 0
                   special_plan: 
       last_throttled_timestamp: NULL
            component_throttled: 
            cancelled_timestamp: NULL
                postpone_launch: 0
                          stage: re-enabling writes
               cutover_attempts: 1
         is_immediate_operation: 0
             reviewed_timestamp: 2024-01-13 14:55:36
1 row in set (0.00 sec)
```
## Cleanup
The `Cleanup` function will delete items related to workflow_name from both `branch_jobs` table and `branch_table_rules` table.
```
Branch -- --workflow_name=<workflow_name> Cleanup
```
### usage
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test cleanup

cleanup workflow:branch_test successfully
```

## gofakeit fucntion
Provides `gofakeit_generate` and `gofakeit_bytype` functions that are fully compatible with Mysql's different types of data generation.

### gofakeit_generate
The `gofakeit_generate` function follows the pattern of `gofakeit.generate` to generate random strings. You can find more information about it on the [gofakeit](https://github.com/brianvoe/gofakeit) GitHub page.
### gofakeit_bytype
`gofakeit_bytype` supports the following functions:

| Type         | Description                                        | Usage Example                              |
|--------------|----------------------------------------------------|--------------------------------------------|
| `tinyint`    | Generates a random tiny integer.                   | `gofakeit_bytype('tinyint')`               |
| `smallint`   | Generates a random small integer.                  | `gofakeit_bytype('smallint')`              |
| `mediumint`  | Generates a random medium integer.                 | `gofakeit_bytype('mediumint')`             |
| `int`        | Generates a random integer.                        | `gofakeit_bytype('int')`                   |
| `integer`    | Alias for `int`.                                   | `gofakeit_bytype('integer')`               |
| `bigint`     | Generates a random big integer.                    | `gofakeit_bytype('bigint')`                |
| `float`      | Generates a random float.                          | `gofakeit_bytype('float')`                 |
| `double`     | Generates a random double.                         | `gofakeit_bytype('double')`                |
| `decimal`    | Generates a random decimal.                        | `gofakeit_bytype('decimal', 5, 2)`         |
| `date`       | Generates a random date.                           | `gofakeit_bytype('date')`                  |
| `datetime`   | Generates a random datetime.                       | `gofakeit_bytype('datetime')`              |
| `timestamp`  | Generates a random timestamp.                      | `gofakeit_bytype('timestamp')`             |
| `time`       | Generates a random time.                           | `gofakeit_bytype('time')`                  |
| `year`       | Generates a random year.                           | `gofakeit_bytype('year')`                  |
| `floatrange` | Generates a random float within a specified range. | `gofakeit_bytype('floatrange', 1.5, 10.0)` |
| `intrange`   | Generates a random int within a specified range.   | `gofakeit_bytype('intrange', 110, 150)`    |
| `name`       | Generates a random name.                           | `gofakeit_bytype('name')`                  |
| `address`    | Generates a random address.                        | `gofakeit_bytype('address')`               |
| `uuid`       | Generates a random UUID.                           | `gofakeit_bytype('uuid')`                  |
| `regex`      | Generates a string matching a regex pattern.       | `gofakeit_bytype('regex', '[a-zA-Z]{5}')`  |
