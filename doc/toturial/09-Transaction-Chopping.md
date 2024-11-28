---
title: Transaction Chopping
---

# Transaction Chopping

## Overview

Transaction Chopping is a feature designed to help you execute large Data Manipulation Language (DML) operations efficiently without overloading your database system. By splitting a large DML operation into smaller batches, you can prevent memory overflows and reduce the impact on other critical SQL operations.

This guide provides a comprehensive overview of Transaction Chopping, including its use cases, how it works, and detailed instructions on how to implement it in your workflow.

## Pre-requisites

Make sure you are connecting to the WeScale endpoint and using `-c` parameter to enable the feature. For example:
```bash
mysql -h127.0.0.1 -P15306 -c
```


## Step 1: Prepare Sample Data

```sql
CREATE DATABASE IF NOT EXISTS test_txn_chopping;

USE test_txn_chopping;

CREATE TABLE mytable (
    id int primary key auto_increment,
    age int
);

INSERT INTO mytable (age)
SELECT FLOOR(RAND() * 100) + 1
FROM (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) x10
CROSS JOIN (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) x100
CROSS JOIN (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) x1000
CROSS JOIN (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) x10000
CROSS JOIN (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) x100000;
```

You should able to see 100,000 rows in the `mytable` table:

```sql
mysql> select count(*) from mytable;
+----------+
| count(*) |
+----------+
|   100000 |
+----------+
1 row in set (0.02 sec)
```

## Step2: Submitting a Transaction Chopping Job

To submit a DML job for Transaction Chopping, you need to:

1. **Add a Directive Comment**: Include `/*vt+ dml_split=true */` in your SQL statement.
2. **Specify Optional Parameters**: Customize execution using additional directives.

**Example:**

```sql
DELETE /*vt+ dml_split=true */ FROM mytable WHERE age >= 10;
```

### Optional Parameters

You can customize the job using the following directives:

| Directive                  | Description                                                         | Example                                  |
|----------------------------|---------------------------------------------------------------------|------------------------------------------|
| `dml_batch_interval`       | Interval between batch executions in milliseconds.                  | `dml_batch_interval=1000`                |
| `dml_batch_size`           | Maximum number of rows per batch.                                   | `dml_batch_size=1000`                    |
| `dml_postpone_launch`      | Postpone job execution until manually launched.                     | `dml_postpone_launch=true`               |
| `dml_fail_policy`          | Batch failure policy: `skip`, `abort`, or `pause`.                  | `dml_fail_policy=pause`                  |
| `dml_time_period_start`    | Start time for job execution (HH:MM:SS).                            | `dml_time_period_start=18:00:00`         |
| `dml_time_period_end`      | End time for job execution (HH:MM:SS).                              | `dml_time_period_end=19:00:00`           |
| `dml_time_period_time_zone`| Time zone for execution times.                                      | `dml_time_period_time_zone=UTC+08:00:00` |
| `dml_throttle_ratio`       | Probability (0-1) of throttling batch execution.                   | `dml_throttle_ratio=0.5`                 |
| `dml_throttle_duration`    | Duration for which throttling is effective.                         | `dml_throttle_duration=30m`              |

**Example with Parameters:**

```sql
DELETE /*vt+ dml_split=true dml_batch_interval=1000 dml_batch_size=1000 dml_fail_policy=pause */ FROM mytable WHERE age >= 10;
```

This job:

- Executes every 1 second.
- Affects up to 1000 rows per batch.
- Pauses the job if a batch fails.

---

## Step3: Monitoring Transaction Chopping Jobs

### Viewing All Jobs

To list all submitted jobs:

```sql
SHOW DML_JOBS\G
```

### Viewing a Specific Job

To view details of a specific job:

```sql
SHOW DML_JOB 'job_uuid'\G
```

### Viewing Batch Details

To view batch details for a job:

```sql
SHOW DML_JOB 'job_uuid' DETAILS\G
```

### Job Fields Explained

**Job Table Fields:**

- `job_uuid`: Unique identifier for the job.
- `table_schema`: Database containing the target table.
- `table_name`: Name of the target table.
- `status`: Current status (e.g., `running`, `paused`).
- `batch_interval_in_ms`: Interval between batch executions.
- `batch_size`: Maximum rows per batch.
- `fail_policy`: Failure handling strategy.
- `affected_rows`: Total rows affected so far.
- `message`: Runtime messages or errors.

**Batch Info Table Fields:**

- `batch_id`: Unique identifier for each batch.
- `batch_status`: Execution status (e.g., `pending`, `completed`).
- `count_size_when_creating_batch`: Estimated rows to be affected.
- `actually_affected_rows`: Actual rows affected after execution.
- `batch_sql`: SQL statement executed for the batch.

---

## Step4: Controlling Transaction Chopping Jobs

### Launching a Postponed Job

If you set `dml_postpone_launch=true`, launch the job manually:

```sql
ALTER DML_JOB 'job_uuid' LAUNCH;
```

### Pausing and Resuming Jobs

- **Pause a Running Job:**

  ```sql
  ALTER DML_JOB 'job_uuid' PAUSE;
  ```

- **Resume a Paused Job:**

  ```sql
  ALTER DML_JOB 'job_uuid' RESUME;
  ```

### Canceling a Job

To cancel a running job:

```sql
ALTER DML_JOB 'job_uuid' CANCEL;
```

**Note:** Canceling a job removes all associated metadata.

### Throttling Batch Execution

Control the execution rate by adjusting the throttling settings.

- **During Job Submission:**

  ```sql
  DELETE /*vt+ dml_split=true dml_throttle_ratio=0.5 dml_throttle_duration=30m */ FROM mytable WHERE age >= 10;
  ```

- **During Job Execution:**

  ```sql
  ALTER DML_JOB 'job_uuid' THROTTLE EXPIRE '30m' RATIO 0.9;
  ```

- **Remove Throttling:**

  ```sql
  ALTER DML_JOB 'job_uuid' UNTHROTTLE;
  ```

### Setting Execution Time Periods

Restrict job execution to specific times:

- **During Job Submission:**

  ```sql
  DELETE /*vt+ dml_split=true dml_time_period_start=18:00:00 dml_time_period_end=19:00:00 dml_time_period_time_zone=UTC+08:00:00 */ FROM mytable WHERE age >= 10;
  ```

- **During Job Execution:**

  ```sql
  ALTER DML_JOB 'job_uuid' TIME_PERIOD 'start_time' 'end_time' 'time_zone';

  -- Example:
  ALTER DML_JOB 'job_uuid' TIME_PERIOD '23:00:00' '06:00:00' 'UTC+08:00:00';
  ```

### Handling Batch Failures

Set the failure policy to define how the job should behave if a batch fails:

- **Policies:**
    - `skip`: Skip the failed batch and continue.
    - `abort`: Mark the job as failed and stop execution.
    - `pause`: Pause the job for manual intervention.

- **Specify During Submission:**

  ```sql
  DELETE /*vt+ dml_split=true dml_fail_policy=abort */ FROM mytable WHERE age >= 10;
  ```

---

## Why Use Transaction Chopping?

Executing large DML operations directly can lead to:

- **Memory Overflows**: Consuming excessive memory resources, potentially crashing the database server.
- **Performance Degradation**: Affecting the performance of other important SQL operations due to high computational load.
- **Operational Risks**: Increasing the likelihood of locks and conflicts, especially during peak business hours.

Transaction Chopping addresses these issues by:

- Splitting large DML operations into smaller, manageable batches.
- Allowing you to schedule DML execution during off-peak hours.
- Providing control over batch execution intervals and sizes.

---

## How Transaction Chopping Works

### DML Splitting

Transaction Chopping divides a large DML operation into multiple smaller batches, each affecting a subset of rows based on the primary key (PK) range. This ensures that each batch operates on a predictable and manageable number of rows.

**Process:**

1. **Job Creation**: The entire DML operation is considered a **job**.
2. **Batch Formation**: The job is split into multiple **batches**, each with a specified `batchSize`.
3. **PK Range Calculation**: Rows are sorted based on the primary key. Each batch covers a specific PK range.
4. **Batch Execution**: Each batch executes the DML operation with an added PK range condition.

**Example:**

Suppose you have a table `mytable` with the following data:

```plaintext
+----+------+
| id | age  |
+----+------+
|  1 |  15  |
|  3 |  10  |
|  6 |  20  |
|  7 |  45  |
|  9 |  56  |
| 10 |  28  |
| 12 |   2  |
| 15 |  23  |
+----+------+
```

You want to execute the following DML:

```sql
DELETE FROM mytable WHERE age >= 10;
```

Using Transaction Chopping with a `batchSize` of `3`, the operation is split into three batches:

1. **Batch 0**:

   ```sql
   DELETE FROM mytable WHERE age >= 10 AND id >= 1 AND id <= 6;
   ```

2. **Batch 1**:

   ```sql
   DELETE FROM mytable WHERE age >= 10 AND id >= 7 AND id <= 10;
   ```

3. **Batch 2**:

   ```sql
   DELETE FROM mytable WHERE age >= 10 AND id >= 15 AND id <= 15;
   ```

Each batch deletes up to 3 rows, ensuring the operation is manageable and less resource-intensive.

### Non-Transactional Nature

It's important to note that Transaction Chopping does **not** guarantee transactional ACID properties:

- **Atomicity**: If a batch fails, previously executed batches are not rolled back.
- **Consistency**: Intermediate states are visible to other transactions.
- **Isolation**: Other transactions may see partial results between batches.
- **Durability**: Completed batches are durable, but failed batches leave partial changes.

**Implications:**

- Ensure your application logic can handle non-atomic operations.
- Be cautious when the order and completeness of data are critical.

---

## Understanding Job Scheduling

### Job Status Lifecycle

Jobs transition through various statuses:

1. `submitted`: Job is created.
2. `preparing`: Preparing batch information.
3. `queued`: Ready to run.
4. `running`: Currently executing batches.
5. `paused`: Execution is paused.
6. `completed`: All batches executed successfully.
7. `failed`: Job failed due to an error.
8. `canceled`: Job was canceled.

**Status Transition Diagram:**

![Job Status Transition](images/Non_transactional_dml_status_transition.png)

### Automatic Batch Splitting

If a batch is estimated to affect more rows than `batch_size`, it is automatically split:

- The current batch executes on the first `batch_size` rows.
- Additional batches are created for the remaining rows.
- New batches have unique identifiers, e.g., `batch_id` becomes `x-2`, `x-3`, etc.

**Benefit:**

- Prevents memory overflow and ensures each batch is manageable.

---

## Best Practices

- **Test Before Execution**: Always test your DML statements on a non-production environment.
- **Monitor Jobs**: Regularly check job and batch statuses to ensure smooth execution.
- **Handle Non-Atomicity**: Be prepared for intermediate states due to the non-transactional nature.
- **Use Time Windows**: Schedule heavy DML operations during off-peak hours.
- **Set Appropriate Batch Sizes**: Balance between execution time and resource utilization.


## Troubleshooting

- **Job Stuck in 'paused' Status**: Check for failure messages and resume the job after resolving issues.
- **Batches Failing Consistently**: Investigate the `batch_sql` for errors or conflicts.
- **Unexpected Data States**: Remember that Transaction Chopping is non-transactional; ensure your application can handle this.


## Conclusion

Transaction Chopping is a powerful feature for handling large DML operations efficiently. By understanding its mechanisms and utilizing its controls, you can execute heavy data modifications with minimal impact on your database performance and stability.

---

## Frequently Asked Questions

### Can I use Transaction Chopping for `INSERT` statements?

No, Transaction Chopping currently supports only `UPDATE` and `DELETE` statements.

### Is there a limit to the number of jobs that can run in parallel?

There is no hard limit, but system resources and performance considerations should guide the number of concurrent jobs.

### How does throttling affect batch execution?

Throttling introduces a delay before executing a batch based on the specified `dml_throttle_ratio`. A ratio of `1` means the batch will always be delayed, while `0` means it will never be throttled.

---

For more advanced configurations and support, please refer to the official documentation or contact the support team.
