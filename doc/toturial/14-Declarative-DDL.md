---
title: Declarative DDL
---

# Declarative DDL

## Overview

The Declarative DDL feature in WeSQL WeScale allows you to manage database schemas in a declarative way. You specify the desired final state of your table, and WeSQL WeScale automatically generates and executes the necessary DDL statements to achieve that state. This approach simplifies schema management and reduces the risk of errors in writing complex DDL statements manually.

## Prerequisites

Before using Declarative DDL, ensure the feature is enabled:

```sql
SET @@enable_declarative_ddl=true;
```
> Notice: If you see an "Unknown system variable" error, you are not connected to the WeScale endpoint. Please connect to the WeScale endpoint and try again.



## Using Declarative DDL

First, create the initial database and table:

```sql
CREATE DATABASE test_declarative_ddl;
       
USE test_declarative_ddl;
    
CREATE TABLE test_declarative_ddl.users ( id INT PRIMARY KEY );
```

Then, you can declare the desired table structure using the `CREATE TABLE` statement. For example, if you want to add a new column `age` of type `INT`, and change the type of id from `INT` to `VARCHAR(64)`, you can use the following statement:

```sql
-- Declare the target structure
CREATE TABLE test_declarative_ddl.users ( id VARCHAR(64) PRIMARY KEY, age INT );
```

Check the final table structure:
```sql
DESC test_declarative_ddl.users;
```

## Configuration Parameters

Declarative DDL behavior can be fine-tuned using the hints parameters. All configuration parameters should be set in the WeScale vtgate configuration file. The format is:

```
parameter_name = value
```

For example:
```
declarative_ddl_hints_auto_increment_strategy = ignore
declarative_ddl_hints_range_rotation_strategy = fullspec
```

### Auto Increment Strategy

Parameter name: `declarative_ddl_hints_auto_increment_strategy`

| Value                    | Description                                                      |
| ----------------------- | ---------------------------------------------------------------- |
| `ignore` (Default)      | Ignores AUTO_INCREMENT property when comparing schemas           |
| `apply_higher`          | Only applies AUTO_INCREMENT changes when the new value is higher |
| `apply_always`          | Always applies AUTO_INCREMENT changes as specified               |

Let's look at some examples to understand how each strategy works. Consider this table structure:

```sql
CREATE TABLE t1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100)
) AUTO_INCREMENT=100;
```

When we want to change the AUTO_INCREMENT value to 200:
```sql
CREATE TABLE t1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100)
) AUTO_INCREMENT=200;
```

1. Using `ignore` strategy:
```sql
-- No DDL will be generated because the ignore strategy ignores AUTO_INCREMENT changes
```

2. Using `apply_always` strategy:
```sql
-- Generated DDL:
ALTER TABLE `t1` AUTO_INCREMENT 200
-- The AUTO_INCREMENT value will always be updated to match the target
```

3. Using `apply_higher` strategy:
```sql
-- Generated DDL:
ALTER TABLE `t1` AUTO_INCREMENT 200
-- The AUTO_INCREMENT value is updated because 200 > 100
```

This strategy is particularly useful when you want to:
- `ignore`: Preserve the existing AUTO_INCREMENT values and handle them separately
- `apply_higher`: Ensure AUTO_INCREMENT values only go up (prevent potential key conflicts)
- `apply_always`: Maintain exact AUTO_INCREMENT values as specified in your schema

### Range Rotation Strategy

Parameter name: `declarative_ddl_hints_range_rotation_strategy`

This strategy determines how WeSQL WeScale handles table partitions, particularly when you want to remove old partitions and add new ones. Think of partitions like filing cabinets for different time periods - you might want to remove cabinets for old years and add new ones for upcoming years.

Partition rotation applies when:
1. Your table uses RANGE partitioning (like organizing data by years)
2. Some old partitions need to be removed
3. Some new partitions need to be added
4. There's at least one partition that stays the same between old and new structure

Available strategies:

| Value                          | Description                                               |
| ----------------------------- | --------------------------------------------------------- |
| `fullspec` (Default)          | Rebuilds the entire partition structure                    |
| `distinct_statements`         | Only removes old partitions when rotation conditions are met |
| `ignore`                      | Keeps existing partitions when rotation conditions are met  |

This strategy is particularly useful when you want to:
- `fullspec`: Ensure the exact partition structure, regardless of performance impact
- `distinct_statements`: Optimize performance by only removing unnecessary partitions
- `ignore`: Preserve existing partitions and only add new ones

Let's look at an example:

Suppose you have a table partitioned by years 2020-2022, and you want to update it to cover 2021-2024:

Original table:
```sql
-- Has partitions for 2020, 2021, 2022
PARTITION BY RANGE (YEAR(created_date)) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023)
);
```

Target structure:
```sql
-- Wants partitions for 2021, 2022, 2023, 2024
PARTITION BY RANGE (YEAR(created_date)) (
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025)
);
```

Here's what each strategy will do:

1. `distinct_statements`: Only removes p2020 partition
   ```sql
   ALTER TABLE `t1` DROP PARTITION `p2020`
   ```
   Why? Because it only removes old partitions when it's safe to do so.

2. `ignore`: Generates no DDL
   ```sql
   -- No DDL generated
   ```
   Why? Because it keeps existing partitions and assumes you'll handle partition management separately.

3. `fullspec`: Rebuilds entire partition structure
   ```sql
   ALTER TABLE `t1`  
   PARTITION BY RANGE (YEAR(`created_date`))  
   (PARTITION `p2021` VALUES LESS THAN (2022),  
    PARTITION `p2022` VALUES LESS THAN (2023),  
    PARTITION `p2023` VALUES LESS THAN (2024),
    PARTITION `p2024` VALUES LESS THAN (2025))
   ```
   Why? Because it ensures the exact partition structure matches your specification.

### Constraint Names Strategy

Parameter name: `declarative_ddl_hints_constraint_names_strategy`

| Value                          | Description |
| ----------------------------- | ----------- |
| `ignore_all` (Default)        | Ignores constraint name differences when the constraint definition is the same |
| `strict`                      | Strictly compares both constraint names and definitions |

This strategy determines how WeScale handles constraint name differences. Important to note:
- Even with `ignore_all`, WeScale will still generate DDL if the constraint definition changes
- `ignore_all` only ignores name differences when the constraint definitions are identical

Let's look at some examples:

Example 1 - Using `strict` strategy, only name changes:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email CHECK (email LIKE '%@%')
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email_format CHECK (email LIKE '%@%')
);

-- Generated DDL: Constraint is recreated because the name changed
ALTER TABLE `t1` 
    DROP CHECK `chk_email`, 
    ADD CONSTRAINT `chk_email_format` CHECK (`email` LIKE '%@%')
```

Example 2 - Using `strict` strategy, definition changes:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email_format CHECK (email LIKE '%@%')
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email_format CHECK (email LIKE '%!%')
);

-- Generated DDL: Constraint is recreated because the definition changed
ALTER TABLE `t1` 
    DROP CHECK `chk_email_format`, 
    ADD CONSTRAINT `chk_email_format` CHECK (`email` LIKE '%!%')
```

Example 3 - Using `ignore_all` strategy, only name changes:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email CHECK (email LIKE '%@%')
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email_format CHECK (email LIKE '%@%')
);

-- Generated DDL: None
-- No DDL because ignore_all ignores name differences when definition is the same
```

Example 4 - Using `ignore_all` strategy, definition changes:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email CHECK (email LIKE '%@%')
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    CONSTRAINT chk_email_format CHECK (email LIKE '%!%')
);

-- Generated DDL: Constraint is recreated because the definition changed
ALTER TABLE `t1` 
    DROP CHECK `chk_email`, 
    ADD CONSTRAINT `chk_email_format` CHECK (`email` LIKE '%!%')
```

### Column Rename Strategy

Parameter name: `declarative_ddl_hints_column_rename_strategy`

| Value                          | Description |
| ----------------------------- | ----------- |
| `heuristic` (Default)         | Makes intelligent decisions about whether columns are being renamed |
| `assume_different`            | Treats columns with different names as completely different columns |

The `heuristic` strategy tries to detect column renames by analyzing column definitions. It will consider a column is being renamed (rather than dropped and added) when:
1. All column properties except the name are identical (including type, charset, collate, nullable, default value, etc.)
2. The columns before and after it are the same in both versions of the table

When these conditions are met, WeScale will generate a `RENAME COLUMN` statement instead of `DROP` and `ADD` statements.

Let's look at two examples:

Example 1 - Column rename is detected:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    username VARCHAR(100)  -- Note the surrounding columns
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,    -- Same column before 'username'
    user_name VARCHAR(100) -- Same properties as 'username', only name is different
);

-- Generated DDL: Simple rename
ALTER TABLE t1 RENAME COLUMN username TO user_name
```
In this example, WeScale detects that:
- The column before 'username' (id) is identical in both versions
- There are no columns after 'username' in either version
- The column properties (VARCHAR(100)) are exactly the same
  Therefore, it treats this as a column rename operation.

Example 2 - Column rename is not detected:
```sql
-- Original table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    username VARCHAR(100),
    tel INT               -- Different following column
);

-- New table
CREATE TABLE t1 (
    id INT PRIMARY KEY,
    user_name VARCHAR(100),
    email VARCHAR(60)     -- Different following column
);

-- Generated DDL: Drop and add
ALTER TABLE t1 
    DROP COLUMN username, 
    DROP COLUMN tel, 
    ADD COLUMN user_name VARCHAR(100), 
    ADD COLUMN email VARCHAR(60)
```
In this example, WeScale does not detect a rename because:
- Even though 'username' and 'user_name' have the same properties
- The columns that follow them are different ('tel' vs 'email')
- When the surrounding columns don't match, WeScale can't be certain if this is a rename operation or if you're restructuring the table
  Therefore, it treats these as separate drop and add operations.

### Alter Table Algorithm Strategy

Parameter name: `declarative_ddl_hints_alter_table_algorithm_strategy`

Specifies the algorithm MySQL should use for executing the DDL. Note that this does not apply to online DDL.

| Value                    | Description                                                |
| ----------------------- | ---------------------------------------------------------- |
| `none` (Default)        | Does not specify an algorithm; MySQL decides automatically |
| `instant`               | Uses `ALGORITHM=INSTANT`                                   |
| `inplace`               | Uses `ALGORITHM=INPLACE`                                   |
| `copy`                  | Uses `ALGORITHM=COPY`                                      |