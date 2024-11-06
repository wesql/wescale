---
title: Declarative DDL
---

# Declarative DDL

## Overview

The Declarative DDL feature in WeSQL WeScale allows you to manage database schemas using a declarative approach. Instead of writing complex DDL statements manually to create or modify tables, you simply define the desired end state of your table. WeSQL WeScale then automatically generates and executes the necessary DDL statements to bring your database schema to that specified state.

This declarative approach offers several benefits:

1. Simplified schema management: You only need to focus on the end result, rather than the step-by-step DDL operations.

2. Reduced risk of errors: By eliminating the need to write intricate DDL statements by hand, Declarative DDL reduces the chances of making syntax or logical mistakes.

3. Easier schema evolution: As your application evolves, updating the database schema becomes more straightforward. You modify the desired state definition, and WeSQL WeScale determines the necessary changes.

Whether you're creating a new table or modifying an existing one, Declarative DDL makes schema management more intuitive and less error-prone. It's particularly useful in scenarios with frequently changing schemas or complex table structures.

## Prerequisites

Before using Declarative DDL, ensure the feature is enabled:

```sql
SET @@enable_declarative_ddl=true;
```
> Notice: If you see an "Unknown system variable" error, you are not connected to the WeScale endpoint. Please connect to the WeScale endpoint and try again.

## Basic Usage
### Using Declarative DDL

To get started with Declarative DDL, first let's create a simple database and table:

```sql
CREATE DATABASE test_declarative_ddl;
       
USE test_declarative_ddl;

CREATE TABLE test_declarative_ddl.users (
    id INT PRIMARY KEY
);
```

Now, suppose you want to modify this table. For example, you might want to:

Add a new column age of type `INT`
Change the type of id from `INT` to `VARCHAR(64)`

With Declarative DDL, you don't need to write the ALTER TABLE statements manually. Instead, you simply declare the desired table structure using the CREATE TABLE statement:

```sql
-- Declare the target structure
CREATE TABLE test_declarative_ddl.users (
    id VARCHAR(64) PRIMARY KEY,
    age INT
);
```
Declarative DDL will automatically figure out the necessary changes and apply them. You won't get a "table already exists" error.

To verify that the changes were applied, you can check the table structure:
```sql
DESC test_declarative_ddl.users;

+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| id    | varchar(64) | NO   | PRI | NULL    |       |
| age   | int         | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+
```
As you can see, the id column type has been changed to VARCHAR(64), and the new age column has been added.

### Previewing Changes with EXPLAIN

Before applying the changes, you might want to see what DDL statements Declarative DDL will generate. You can do this using the `EXPLAIN` command.

For example, suppose you want to change your table to:
```sql
CREATE TABLE test_declarative_ddl.users (
    id INT PRIMARY KEY,
    height INT
);
```
To see the DDL statements that will be generated, use:
```sql
EXPLAIN CREATE TABLE test_declarative_ddl.users (
  id INT PRIMARY KEY,
  height INT
);
```
You will get output like:
```sql
+-------------------------------------------------------------------------------------------------------------+
| DDLs to Execute                                                                                             |
+-------------------------------------------------------------------------------------------------------------+
| ALTER TABLE `test_declarative_ddl`.`users` MODIFY COLUMN `id` int NOT NULL, RENAME COLUMN `age` TO `height` |
+-------------------------------------------------------------------------------------------------------------+
1 row in set (2.93 sec)
@@enable_declarative_ddl is true, @@ddl_strategy is direct
```
> Note that EXPLAIN CREATE TABLE shows the DDL statements that are going to be executed by WeScale. You can use this even when `@@enable_declarative_ddl` is false. When `@@enable_declarative_ddl` is false, it will print the original create table DDL you want to execute.

After previewing the changes, you can either run the DDL statements shown in the output, or simply run the declarative CREATE TABLE statement:
```sql
CREATE TABLE test_declarative_ddl.users (
  id INT PRIMARY KEY,
  height INT
);
```

To verify the changes:
```sql
DESC test_declarative_ddl.users;

+--------+------+------+-----+---------+-------+
| Field  | Type | Null | Key | Default | Extra |
+--------+------+------+-----+---------+-------+
| id     | int  | NO   | PRI | NULL    |       |
| height | int  | YES  |     | NULL    |       |
+--------+------+------+-----+---------+-------+
```

### Best Practices

1. Use `EXPLAIN CREATE TABLE` to preview the DDL statements that Declarative DDL will generate before executing them.
2. Declarative DDL is a feature help generate DDLs needed from current table structure to target table structure, it's independent of DDL execution strategy. You can set `@@ddl_strategy` to `direct` or `online` based on your needs.
3. If you want more control over the DDL statements generated by Declarative DDL, see the Configuration Parameters chapter.

## Configuration Parameters (Advanced)
The Declarative DDL hints parameters provide more control over the behavior of Declarative DDL. Feel free to explore these options as you become more familiar with the feature. 

All configuration parameters should be set in the WeScale vtgate configuration file. The format is:

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