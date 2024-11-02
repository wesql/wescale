---
title: Declarative DDL
---

# Declarative DDL

## Overview

The Declarative DDL feature in WeSQL WeScale allows you to manage database schemas in a declarative way. You specify the desired final state of your table, and WeSQL WeScale automatically generates and executes the necessary DDL statements to achieve that state. This approach simplifies schema management and reduces the risk of errors in writing complex DDL statements manually.

## Configuration

### Enabling Declarative DDL

To enable declarative DDL, set the following system variable:

```sql
SET @@enable_declarative_ddl=true;
```

You can use Declarative DDL with both direct and online DDL strategies:

```sql
-- For direct DDL execution
SET @@ddl_strategy='direct';

-- For online DDL execution
SET @@ddl_strategy='online';
```

## Usage Example

Let's walk through a simple example:

1. Create an initial table:

    ```sql
    CREATE TABLE users (id INT PRIMARY KEY);
    ```

2. Declare the desired schema state:

    ```sql
    CREATE TABLE users (
        id VARCHAR(64) PRIMARY KEY,
        age INT
    );
    ```

   WeSQL WeScale will automatically:
   - Modify the `id` column from `INT` to `VARCHAR(64)`
   - Add the new `age` column

## Configuration Parameters

Declarative DDL behavior can be fine-tuned using the following configuration parameters. Modify the configuration file with the appropriate key-value pairs, for example:

```cnf
declarative_ddl_hints_auto_increment_strategy = apply_always
```

### Auto Increment Strategy

Parameter name: `declarative_ddl_hints_auto_increment_strategy`

| Value             | Description                                                      |
| ----------------- | ---------------------------------------------------------------- |
| `ignore`(default) | Ignores AUTO_INCREMENT property when comparing schemas           |
| `apply_higher`    | Only applies AUTO_INCREMENT changes when the new value is higher |
| `apply_always`    | Always applies AUTO_INCREMENT changes as specified               |

### Range Rotation Strategy

Parameter name: `declarative_ddl_hints_range_rotation_strategy`

> **Note**: The partition rotation strategy **only takes effect when partition rotation conditions are met.** Partition rotation applies when:

- Both tables use RANGE partitioning
- There is at least one continuous shared partition sequence between the source and target tables
- Some leading partitions in the source table do not exist in the target table (have been deleted)
- The target table may have additional new partitions

When partition rotation conditions **are not met**, it will directly modify the source table's partitions to match the target table's structure, similar to the `fullspec` approach.

| Value                 | Description                                                                                           |
| --------------------- | ----------------------------------------------------------------------------------------------------- |
| `fullspec`(default)   | Modifies partition structure directly without optimizing from shared partitions.                      |
| `distinct_statements` | Generates statements to delete excess partitions from the source table if partition rotation applies. |
| `ignore`              | Ignores differences caused by partition discrepancy when partition rotation applies.                  |

**Example:**  
Source table structure:
```sql
CREATE TABLE t1 (
    id INT,
    name VARCHAR(100),
    created_date DATE
)
PARTITION BY RANGE (YEAR(created_date)) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023)
);
```

Target table structure:
```sql
CREATE TABLE t2 (
    id INT,
    name VARCHAR(100),
    created_date DATE
)
PARTITION BY RANGE (YEAR(created_date)) (
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025)
);
```

Generated DDL with different strategies:

Using `distinct_statements`:
```sql
ALTER TABLE `t1` DROP PARTITION `p2020`
```

Using `ignore`:
```sql
-- No DDL generated
```

Using `fullspec`:
```sql
ALTER TABLE `t1`  
PARTITION BY RANGE (YEAR(`created_date`))  
(PARTITION `p2021` VALUES LESS THAN (2022),  
 PARTITION `p2022` VALUES LESS THAN (2023),  
 PARTITION `p2023` VALUES LESS THAN (2024),
 PARTITION `p2024` VALUES LESS THAN (2025))
```

### Constraint Names Strategy

Parameter name: `declarative_ddl_hints_constraint_names_strategy`

| Value             | Description                        |
| ----------------- | ---------------------------------- |
| `ignore_all`      | Ignores differences in constraints |
| `strict`(default) | Strictly compares constraints      |

### Column Rename Strategy

Parameter name: `declarative_ddl_hints_column_rename_strategy`

| Value                | Description                                              | Effect                                                                          |
| -------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `assume_different`   | Treats columns with different names as different columns | Generates `DROP COLUMN` and `ADD COLUMN` statements                             |
| `heuristic`(default) | Makes heuristic judgments when column names differ       | Generates `CHANGE COLUMN` statements when column position and type are the same |

The heuristic judgment process: If a column in the source table and a column in the target table are identical except for their names, and their preceding and following columns are consistent, then these columns can be transformed using `CHANGE COLUMN` instead of `DROP COLUMN` followed by `ADD COLUMN`.

Examples:

Example 1:
```sql
-- Schema 1
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    username VARCHAR(100)
);

-- Schema 2
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    user_name VARCHAR(100)
);
```

Meets heuristic conditions, generated DDL:
```sql
ALTER TABLE t1 RENAME COLUMN username TO user_name
```

Example 2:
```sql
-- Schema 1
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    username VARCHAR(100),
    tel INT
);

-- Schema 2
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    user_name VARCHAR(100),
    email VARCHAR(60)
);
```

Does not meet heuristic conditions because the columns following `username` and `user_name` are different. Generated DDL:
```sql
ALTER TABLE t1 
    DROP COLUMN username, 
    DROP COLUMN tel, 
    ADD COLUMN user_name VARCHAR(100), 
    ADD COLUMN email VARCHAR(60)
```

### Alter Table Algorithm Strategy

Parameter name: `declarative_ddl_hints_alter_table_algorithm_strategy`

Specifies the algorithm MySQL should use for executing the DDL. Note that this does not apply to online DDL.

| Value           | Description                                                |
| --------------- | ---------------------------------------------------------- |
| `none`(default) | Does not specify an algorithm; MySQL decides automatically |
| `instant`       | Uses `ALGORITHM=INSTANT`                                   |
| `inplace`       | Uses `ALGORITHM=INPLACE`                                   |
| `copy`          | Uses `ALGORITHM=COPY`                                      |
