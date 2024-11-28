---
title: Filters
---

# Filters

## Overview

Filters in WeScale act as an interception layer for SQL queries, allowing you to control and modify their execution based on predefined rules, similar to the filter pattern used in API Gateways. By defining filter rules, you can control which queries are executed, modify their content, or apply specific actions based on predefined policies such as concurrency control and security measures.

Each filter consists of two parts:

- **Pattern Matching**: Identify and intercept queries based on `SQL operation types` (SELECT, INSERT, UPDATE, DELETE), `targeted tables` or `databases`, `user identities`, `IP addresses`, and specific `query patterns`.
- **Action Execution**: Execute predefined actions when a query matches a filter rule, such as `rejecting the query`, `modifying its content`, `limiting its concurrency`, or `custom wasm plugin`.

## Prerequisites

Make sure you are connecting to the WeScale endpoint by executing the following command:

```sql
mysql> show filters;
Empty set (0.01 sec)
```

If you see an empty set, you are ready to create and manage filters. If you encounter an error, you are not connected to the WeScale endpoint.

## Step1: Example Scenario Setup

We'll use a sample environment to demonstrate the creation and usage of filters. Consider the following database and table:

```sql
create database test_filter;
use test_filter;
create table test_filter.t1 (c1 int primary key auto_increment, c2 int);
insert into test_filter.t1 values (1, 1);

create table test_filter.t2 like t1;
insert into test_filter.t2 values (2, 2);
```

## Step2: Creating a Filter

To create a filter in WeScale, use the `CREATE FILTER` statement. Filters consist of three main parts:

1. **Metadata**: Basic information about the filter.
2. **Pattern Matching**: Criteria that determine which queries the filter applies to.
3. **Action Execution**: The action taken when a query matches the pattern.

### Example: Blocking Specific Queries

The following example creates a filter named `block_select_insert` that blocks `SELECT` and `INSERT` operations on the table `test_filter.t1`.

```sql
CREATE FILTER block_select_insert (
    desc = 'Blocks SELECT and INSERT operations on test_filter.t1',
    priority = '1000',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Select,Insert',
    fully_qualified_table_names = 'test_filter.t1'
)
EXECUTE (
    action = 'FAIL'
);
```

**Explanation:**

- **Metadata**:
  - `desc`: Description of the filter.
  - `priority`: Determines the order in which filters are applied (lower value means higher priority).
  - `status`: Can be `ACTIVE` or `INACTIVE`.

- **Pattern Matching**:
  - `plans`: Specifies the SQL operations to intercept.
  - `fully_qualified_table_names`: Targets the specific table `test_filter.t1`.

- **Action Execution**:
  - `action`: Specifies the action to take when a query matches the pattern. In this case, `FAIL` causes the query to be rejected.

### Example: Blocking All Delete Queries

To block all `DELETE` queries on the table `test_filter.t2`, create a filter named `block_delete`:

```sql
CREATE FILTER block_delete (
    desc = 'Blocks all delete queries on test_filter.t2',
    priority = '900',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Delete',
    fully_qualified_table_names = 'test_filter.t2'
)
EXECUTE (
    action = 'FAIL'
);
```

### Full Syntax of `CREATE FILTER` Statement

<details>
<summary><strong>Full Syntax of `CREATE FILTER` Statement</strong></summary>

```sql
CREATE FILTER [IF NOT EXISTS] filter_name (
    desc = 'Filter description',
    priority = '1000',          -- Default is 1000
    status = 'ACTIVE'           -- 'ACTIVE' or 'INACTIVE', default is 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Select,Insert',    -- 'Select', 'Insert', 'Update', 'Delete'
    fully_qualified_table_names = '',
    query_regex = '',
    query_template = '',
    request_ip_regex = '',
    user_regex = '',
    leading_comment_regex = '',
    trailing_comment_regex = ''
)
EXECUTE (
    action = 'FAIL',            -- Possible values: 'CONTINUE', 'FAIL', 'CONCURRENCY_CONTROL'
    action_args = ''
);
```

- **plans**: Specifies the SQL operation types to match. Possible values are `Select`, `Insert`, `Update`, `Delete`. Multiple types can be separated by commas.

- **fully_qualified_table_names**: Matches specific tables. Provide fully qualified table names in the format `database.table`. Multiple tables can be separated by commas.

- **query_regex**: Matches queries using a regular expression. Useful for matching specific query patterns.

- **query_template**: Matches queries based on a template with placeholders, e.g., `SELECT * FROM t1 WHERE id = :id`.

- **request_ip_regex**: Matches queries based on the client's IP address using a regular expression.

- **user_regex**: Matches queries based on the database user using a regular expression.

- **leading_comment_regex**: Matches queries based on comments at the beginning of the query.

- **trailing_comment_regex**: Matches queries based on comments at the end of the query.

</details>


## Step3: Tesing Filters

Once a filter is active, it automatically applies to matching queries.

**Test the `block_select_insert` Filter:**

```sql
SELECT * FROM test_filter.t1;
```

**Expected Result:** We expect the query to fail since the filter blocks `SELECT` operations on `test_filter.t1`.

```plaintext
ERROR 1105 (HY000): target: test_filter.0.primary: vttablet: rpc error: code = InvalidArgument desc = disallowed due to rule: block_select_insert (CallerID: userData1)
```

**Test the `block_delete` Filter:**

```sql
delete from test_filter.t2 where c1 > 0;
```

**Expected Result:** We expect the query to fail since the filter blocks `DELETE` operations on `test_filter.t2`.

```plaintext
ERROR 1105 (HY000): target: test_filter.0.primary: vttablet: rpc error: code = InvalidArgument desc = disallowed due to rule: block_delete (CallerID: userData1)
```


## Step4: Explaining Filters

To determine which filters apply to a specific query, use the `/*explain filter*/` comment before your SQL statement.

**Example:**
> NOTICE: Make sure you are connecting to MySQL using `-c` parameter, otherwise the `/*explain filter*/` comment will be ignored.
```sql
/*explain filter*/ SELECT * FROM test_filter.t1;
```

**Sample Output:**

```sql
mysql> /*explain filter*/ SELECT * FROM test_filter.t1;
+---------------------+----------------------------------------------+----------+--------+-------------+
| Name                | description                                  | priority | action | action_args |
+---------------------+----------------------------------------------+----------+--------+-------------+
| block_select_insert | Blocks SELECT and INSERT operations on test_filter.t1 | 1000     | FAIL   |             |
+---------------------+----------------------------------------------+----------+--------+-------------+
1 row in set (0.00 sec)
```


```sql
/*explain filter*/ DELETE FROM test_filter.t2 where c1 > 0;
```

**Sample Output:**

```sql
+--------------+------------------------------------+----------+--------+-------------+
| Name         | description                        | priority | action | action_args |
+--------------+------------------------------------+----------+--------+-------------+
| block_delete | Blocks all delete queries on test_filter.t2 | 900      | FAIL   |             |
+--------------+------------------------------------+----------+--------+-------------+
1 row in set (0.004 sec)
```

---

## Step5: Managing Filters

### Viewing Filters

To list all existing filters:

```sql
SHOW FILTERS;
```

**Sample Output:**

```sql
MySQL [test_filter]> SHOW FILTERS\G;
*************************** 1. row ***************************
                         id: 4
           create_timestamp: 2024-09-29 09:18:50
           update_timestamp: 2024-09-29 09:18:50
                       name: block_select_insert
                description: Blocks SELECT and INSERT operations on test_filter.t1
                   priority: 1000
                     status: ACTIVE
                      plans: ["Select","Insert"]
fully_qualified_table_names: ["test_filter.t1"]
                query_regex:
             query_template:
           request_ip_regex:
                 user_regex:
      leading_comment_regex:
     trailing_comment_regex:
             bind_var_conds:
                     action: FAIL
                action_args:
*************************** 2. row ***************************
                         id: 7
           create_timestamp: 2024-09-29 09:30:12
           update_timestamp: 2024-09-29 09:30:12
                       name: block_delete
                description: Blocks all delete queries on test_filter.t2
                   priority: 900
                     status: ACTIVE
                      plans: ["Delete"]
fully_qualified_table_names: ["test_filter.t2"]
                query_regex:
             query_template:
           request_ip_regex:
                 user_regex:
      leading_comment_regex:
     trailing_comment_regex:
             bind_var_conds:
                     action: FAIL
                action_args:
```

### Viewing Filter Details

To see the full definition of a specific filter:

```sql
SHOW CREATE FILTER block_select_insert\G
```

**Sample Output:**

```sql
mysql> SHOW CREATE FILTER block_select_insert\G
*************************** 1. row ***************************
        Filer: block_select_insert
Create Filter:
create filter block_select_insert (
	desc='Blocks SELECT and INSERT operations on test_filter.t1',
	priority='1000',
	status='ACTIVE'
)
with_pattern (
	plans='Select,Insert',
	fully_qualified_table_names='test_filter.t1',
	query_regex='',
	query_template='',
	request_ip_regex='',
	user_regex='',
	leading_comment_regex='',
	trailing_comment_regex='',
	bind_var_conds=''
)
execute (
	action='FAIL',
	action_args=''
)
1 row in set (0.01 sec)
```

---

## Step6: Deleting a Filter

To remove a filter:

```sql
DROP FILTER block_select_insert;
```


## Step7: Modifying a Filter

Use the `ALTER FILTER` statement to modify an existing filter.

### Example: Updating a Filter

Let's update the description and action of the filter `block_select_insert`.

```sql
ALTER FILTER block_delete (
    desc = 'Updated filter to control concurrency on test_filter.t2',
    priority = '900',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Delete',
    fully_qualified_table_names = 'test_filter.t2'
)
EXECUTE (
    action = 'CONCURRENCY_CONTROL',
    action_args = 'max_concurrency=0; max_queue_size=0'
);
```

### Testing the Updated Filter

After updating the filter, test it by executing a `DELETE` query on the table `test_filter.t2`. You can see the output has changed to reflect the new action.

```sql
MySQL [test_filter]> delete from t2 where c1 > 0;
ERROR 1203 (42000): target: test_filter.0.primary: vttablet: rpc error: code = ResourceExhausted desc = concurrency control protection: too many queued transactions (0 >= 0) (CallerID: userData1)
```

**Explanation:**

- **Metadata**: Updated the description to reflect the new purpose.
- **Action Execution**: Changed the action to `CONCURRENCY_CONTROL` with specified arguments.

### Full Syntax of `ALTER FILTER` Statement
The syntax is similar to `CREATE FILTER`.
```sql
ALTER FILTER filter_name (
    desc = '',              -- Optional: Update description
    priority = '',          -- Optional: Update priority
    status = ''             -- Optional: 'ACTIVE' or 'INACTIVE'
)
WITH_PATTERN (
    plans = '',             -- Optional: Update plans
    fully_qualified_table_names = '',
    query_regex = '',
    query_template = '',
    request_ip_regex = '',
    user_regex = '',
    leading_comment_regex = '',
    trailing_comment_regex = ''
)
EXECUTE (
    action = '',            -- Optional: Update action
    action_args = ''
);
```

---

## Built-in Actions

WeScale currently supports the following built-in actions:

<details>
<summary><strong>CONTINUE</strong></summary>

- **Description**: Allows the query to proceed without any interruption.
- **Usage**: No action arguments are needed.

**Example of Using `CONTINUE`:**

```sql
CREATE FILTER continue_example (
    desc = 'Do nothing',
    priority = '800',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Update,Delete',
    fully_qualified_table_names = 'test_filter.t1'
)
EXECUTE (
    action = 'CONTINUE',
);
```

</details>

<details>
<summary><strong>FAIL</strong></summary>

- **Description**: Rejects the query, causing it to fail.
- **Usage**: No action arguments are needed.

**Example of Using `FAIL`:**

```sql
CREATE FILTER fail_example (
    desc = 'Block UPDATE and DELETE operations on test_filter.t1',
    priority = '800',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Update,Delete',
    fully_qualified_table_names = 'test_filter.t1'
)
EXECUTE (
    action = 'FAIL',
);
```

</details>

<details>
<summary><strong>CONCURRENCY_CONTROL</strong></summary>

- **Description**: Controls the concurrency of matching queries.
- **Action Arguments**:
  - `max_concurrency`: Maximum number of concurrent queries allowed.
  - `max_queue_size`: Maximum number of queries allowed in the queue, including executing queries.

**Example of Using `CONCURRENCY_CONTROL`:**

```sql
CREATE FILTER concurrency_control_example (
    desc = 'Limits concurrency on UPDATE and DELETE operations on test_filter.t1',
    priority = '800',
    status = 'ACTIVE'
)
WITH_PATTERN (
    plans = 'Update,Delete',
    fully_qualified_table_names = 'test_filter.t1'
)
EXECUTE (
    action = 'CONCURRENCY_CONTROL',
    action_args = 'max_concurrency=10; max_queue_size=100'
);
```

</details>

## Best Practices

- **Prioritize Filters**: Use the `priority` parameter to ensure critical filters are evaluated first. Lower values indicate higher priority.
- **Test Filters**: Before deploying filters to production, test them using `/*explain filter*/` to verify they match the intended queries.
- **Monitor Performance**: Be cautious with complex regular expressions, as they may impact performance.
- **Keep Descriptions Updated**: Use the `desc` field to provide clear explanations of each filter's purpose.


## Conclusion

Filters in WeScale provide powerful tools for controlling SQL query execution, enhancing security, and managing resources. By carefully defining patterns and actions, you can enforce policies and optimize database performance.

For more detailed information on filter design and advanced configurations, refer to the WeScale [Filter Design Document](https://github.com/wesql/wescale/blob/main/doc/design/20240514_Filters.md).

