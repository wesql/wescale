---
title: Online DDL
---

# Online DDL

## Overview

WeScale introduces a powerful and efficient way to handle schema migrations in MySQL through its Online DDL feature. This guide will walk you through the essentials of using Online DDL in WeScale, highlighting its benefits and providing step-by-step instructions for common tasks.

Traditional MySQL DDL operations can be blocking and resource-intensive, impacting database performance and availability. WeScale's Online DDL addresses these challenges by offering:

- **Non-blocking and Asynchronous Operations**: Perform schema changes without locking tables or interrupting database operations.
- **Automated Workflow**: Reduce manual effort with automated statement construction, scheduling, execution, and progress monitoring.
- **Simplified Schema Migrations**: Use standard `ALTER TABLE` statements for intuitive schema changes.
- **Enhanced Resource Management**: Benefit from graceful resource usage, better interruption handling, and improved failure recovery.

## Prerequisites

Make sure you are connecting to the WeScale endpoint and have the `ddl_strategy` variable set to `online`. You can verify this by running the following commands:

```sql
mysql> SET @@ddl_strategy='online';
Query OK, 0 rows affected (0.01 sec)

mysql> SELECT @@ddl_strategy;
+----------------+
| @@ddl_strategy |
+----------------+
| online         |
+----------------+
1 row in set (0.01 sec)

-- To Disable Online DDL, You can set the ddl_strategy back to 'direct'
```

> **Notice**: If you see an "Unknown system variable" error, you are not connected to the WeScale endpoint. Please connect to the WeScale endpoint and try again.

## Step1: Performing an Online DDL Operation

With the DDL strategy set, you can perform schema migrations using standard SQL statements. Here's how to execute an `ALTER TABLE` operation on a sample business table.

First, let's consider a `customers` table:

```sql
CREATE DATABASE IF NOT EXISTS test_onlineddl;

USE test_onlineddl;

CREATE TABLE customers (
  customer_id int NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

## Step2: Inserting Sample Data

Let's insert some sample data into the `customers` table. We use a cross join to generate 100000 rows:

```sql
INSERT INTO customers (name, email)
SELECT 
    CONCAT(ROW_NUMBER() OVER (), '_customer'),
    CONCAT(ROW_NUMBER() OVER (), '_customer@example.com')
FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x10
CROSS JOIN (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x100
CROSS JOIN (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x1000
CROSS JOIN (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x10000
CROSS JOIN (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x100000;
```

## Step3: Performing the Online Schema Change

Suppose we need to add a new column `phone_number` to store customers' phone numbers. We can perform an online `ALTER TABLE` operation:

```sql
mysql> ALTER TABLE customers ADD COLUMN phone_number VARCHAR(20);
+--------------------------------------+
| uuid                                 |
+--------------------------------------+
| d2bf1bba_92a5_11ef_8f37_fac873d3e6cd |
+--------------------------------------+
1 row in set (0.01 sec)
```

Upon execution, WeScale will:

1. **Submit a Schema Migration Job**: The `ALTER TABLE` operation is queued as a migration job.
2. **Execute Asynchronously**: The operation runs without blocking the table, allowing uninterrupted database access.
3. **Provide Tracking UUID**: A unique identifier (UUID) is returned for monitoring the migration progress.

## Step4: Monitoring Migration Progress

To monitor the status of your schema migration job, use the UUID provided after initiating the operation:

```sql
mysql> SHOW SCHEMA_MIGRATION LIKE 'd2bf1bba_92a5_11ef_8f37_fac873d3e6cd'\G
*************************** 1. row ***************************
                             id: 5
                 migration_uuid: d2bf1bba_92a5_11ef_8f37_fac873d3e6cd
                       keyspace: test_onlineddl
                          shard: 0
                   mysql_schema: test_onlineddl
                    mysql_table: customers
            migration_statement: alter table test_onlineddl.customers add column phone_number VARCHAR(20)
                       strategy: online
                        options:
                added_timestamp: 2024-10-25 07:50:37
            requested_timestamp: 2024-10-25 07:50:37
                ready_timestamp: NULL
              started_timestamp: 2024-10-25 07:50:39
             liveness_timestamp: 2024-10-25 07:50:45
            completed_timestamp: 2024-10-25 07:50:45.755498
              cleanup_timestamp: NULL
               migration_status: complete
           status_before_paused: NULL
                       log_path:
                      artifacts: _vt_HOLD_d5c0f46492a511ef8a29fac873d3e6cd_20241026075042,_d2bf1bba_92a5_11ef_8f37_fac873d3e6cd_20241025155038_vrepl,
                        retries: 0
                         tablet: zone1-0000000000
                 tablet_failure: 0
                       progress: 100
              migration_context: vtgate:bbcd8b3a-92a5-11ef-8f37-fac873d3e6cd,online_ddl:d2bf19d0-92a5-11ef-8f37-fac873d3e6cd
                     ddl_action: alter
                        message:
                    eta_seconds: 0
                    rows_copied: 100000
                     table_rows: 99870
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
      vitess_liveness_indicator: 1729842642
            user_throttle_ratio: 0
                   special_plan:
       last_throttled_timestamp: NULL
            component_throttled:
            cancelled_timestamp: NULL
                postpone_launch: 0
                          stage: re-enabling writes
               cutover_attempts: 1
         is_immediate_operation: 0
             reviewed_timestamp: 2024-10-25 07:50:39
1 row in set (0.00 sec)
```

This output provides detailed information about the schema migration, including:

- **migration_statement**: Current DDL Statemnt being executed.
- **Migration Status**: Current state (e.g., `queued`, `running`, `complete`).
- **Progress**: Percentage of completion.
- **Timestamps**: Various timestamps indicating the lifecycle of the migration.
- **Rows Copied**: Number of rows processed during the migration.

## Step5: Controlling Migrations

Schema migration jobs may take a significant amount of time to complete. Especially for large tables, it's essential to manage migrations effectively.
WeScale allows you to manage migrations actively, ensuring you have full control over the process.

### Pause a Migration

Pausing a migration can be useful if you need to temporarily halt changes without canceling them:

```sql
ALTER SCHEMA_MIGRATION 'd2bf1bba_92a5_11ef_8f37_fac873d3e6cd' PAUSE;
```

### Resume a Migration

To resume a paused migration:

```sql
ALTER SCHEMA_MIGRATION 'd2bf1bba_92a5_11ef_8f37_fac873d3e6cd' RESUME;
```


### Cancel a Migration

To cancel a specific migration:

```sql
ALTER SCHEMA_MIGRATION 'd2bf1bba_92a5_11ef_8f37_fac873d3e6cd' CANCEL;
```

### Cancel All Pending Migrations

This command cancels all migrations in `queued`, `ready`, or `running` statuses:

```sql
ALTER SCHEMA_MIGRATION CANCEL ALL;
```

## How Online DDL Works in WeScale

WeScale's Online DDL feature allows you to perform schema migrations with minimal impact on your database's performance and availability. Here's a simplified explanation of how it works:

1. **Copying Existing Data (Initial Data Copy)**:
  - When you initiate an Online DDL operation, WeScale creates a **shadow table** that includes the new schema changes.
  - The existing data from the original table is **copied** to the shadow table in the background.
  - This copying process is non-blocking, meaning it doesn't lock the original table, so your applications can continue to read from and write to it without interruption.

2. **Capturing Incremental Changes (Ongoing Data Copy)**:
  - While the initial data copy is underway, any new changes (inserts, updates, deletes) made to the original table are **captured**.
  - These incremental changes are continuously **applied** to the shadow table to keep it in sync with the original table.
  - This ensures that no data is lost and the shadow table remains an up-to-date replica.

3. **Cutover (Switching Tables)**:
  - Once the shadow table has caught up with all the changes and is fully synchronized with the original table, WeScale prepares for the **cutover** phase.
  - During the cutover, the original table is briefly locked to ensure data consistency.
  - The shadow table is then **swapped** in place of the original table. This operation is typically very fast, minimizing the lock time.
  - After the swap, the new table (with the schema changes) becomes active, and the original table is cleaned up.

By following this process, WeScale ensures that schema migrations are **non-disruptive** and **efficient**, allowing you to implement changes without significant downtime or performance hits. The combination of initial data copy, incremental change capture, and quick cutover makes it possible to maintain high availability during schema migrations.
