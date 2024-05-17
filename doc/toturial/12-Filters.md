# Introduction
Filter Pattern is commonly used in API Gateways to intercept and manipulate requests and responses.
Users can define a set of rules to filter out wanted requests and modify the request before it reaches the backend service or after the response is received.

WeScale, as a developer-friendly database proxy, provides a similar feature for users to filter out wanted SQL queries and modify the SQL queries before or after they are executed.

For the detailed information of the design, please refer to the [design document](..%2Fdesign%2F20240514_Filters.md)

# Create a Filter
The syntax of creating a filter is simple and straightforward. For example, to create a filter named `foo`, you can use the following SQL command:
```sql
create filter foo(
        desc='The description of the filter foo',
        priority='1000',
        status='ACTIVE'
)
with_pattern(
        plans='Select,Insert',
        fully_qualified_table_names='d1.t1'
)
execute(
        action='FAIL'
);
```

Let's break down the above command:
- `create filter foo`: This command creates a filter named `foo`.
- The first block of the filter definition is the metadata part of the filter:
    - `desc='The description of the filter foo'`: This is the description of the filter.
    - `priority='1000'`: This is the priority of the filter. The smaller the value, the higher the priority.
    - `status='ACTIVE'`: This is the status of the filter. The value can be `ACTIVE` or `INACTIVE`.
- The second block of the filter definition is the pattern matching part of the filter:
    - `plans='Select,Insert'`: This is the type of SQL queries that the filter will match. The value can be `Select`, `Insert`, `Update`, `Delete`.
    - `fully_qualified_table_names='d1.t1'`: This is the fully qualified table name that the filter will match.
- The third block of the filter definition is the action part of the filter:
    - `action='FAIL'`: This is the action that the filter will take when a match is found. `FAIL` means the SQL query will fail when the filter matches.

# Full Syntax of `create filter` statement
The full syntax of the `create filter` statement is as follows:
```sql
create filter [if not exists] $name(
        desc='',
        priority='1000', -- default 1000
        status='ACTIVE' -- ACTIVE or INACTIVE, default ACTIVE
)
with_pattern(
        plans='Select,Insert', -- Select, Insert, Update, Delete
        fully_qualified_table_names='',
        query_regex='', -- Regular expression for matching the query
        query_template='', -- Query template for matching the query, e.g., select * from t1 where id = :id
        request_ip_regex='', -- Regular expression for matching the request IP
        user_regex='', -- Regular expression for matching the user
        leading_comment_regex='', -- Regular expression for matching the leading comment of the query
        trailing_comment_regex='' -- Regular expression for matching the trailing comment of the query
)
execute(
        action='FAIL', -- action to take when the filter matches, possible values: CONTINUE, FAIL, CONCURRENCY_CONTROL
        action_args='' -- action arguments for the action
);
```

# Alter a Filter
To alter a filter, you can simply use the `alter filter` statement. For example, to change the description of the filter `foo`, you can use the following SQL command:
```sql
alter filter foo(
        desc='The new description of the filter foo, haha',
        priority='1000',
        status='ACTIVE'
)
with_pattern(
        plans='Select,Insert',
        fully_qualified_table_names='d1.t1'
)
execute(
        action='FAIL'
);
```

# Show Filters
To show all the filters, you can use the `show filters` statement. For example:
```sql
mysql> show filters\G
*************************** 1. row ***************************
                         id: 4
           create_timestamp: 2024-05-17 15:29:31
           update_timestamp: 2024-05-17 15:29:31
                       name: foo
                description: The description of the filter foo
                   priority: 1000
                     status: ACTIVE
                      plans: ["Select","Insert"]
fully_qualified_table_names: ["d1.t1"]
                query_regex:
             query_template:
           request_ip_regex:
                 user_regex:
      leading_comment_regex:
     trailing_comment_regex:
             bind_var_conds:
                     action: FAIL
                action_args:
1 row in set (0.01 sec)
```

# Explain a Filter
For a specific SQL query, you can use the `/*explain filter*/` leading comment to explain which filter matches the query. For example:
```sql
mysql> /*explain filter*/select * from t1;
+------+-----------------------------------+----------+--------+-------------+
| Name | description                       | priority | action | action_args |
+------+-----------------------------------+----------+--------+-------------+
| foo  | The description of the filter foo | 1000     | FAIL   |             |
+------+-----------------------------------+----------+--------+-------------+
1 row in set (0.00 sec)


mysql> select * from t1;
ERROR 1105 (HY000): target: d1.0.primary: vttablet: rpc error: code = InvalidArgument desc = disallowed due to rule: foo (CallerID: userData1)
```

# Drop a Filter
To drop a filter, you can use the `drop filter` statement. For example, to drop the filter `foo`, you can use the following SQL command:
```sql
drop filter foo;
```

# Actions
WeScale currently supports the following builtin actions:
- `CONTINUE`: Continue to execute the SQL query. It takes no arguments.
- `FAIL`: Fail the SQL query. It takes no arguments.
- `CONCURRENCY_CONTROL`: Control the concurrency of the SQL query. It takes the following arguments:
    - `max_concurrency`: The maximum number of concurrent queries allowed.
    - `max_queue_size`: The maximum number of queries allowed in the queue, including the executing queries.

An example of using the `CONCURRENCY_CONTROL` action is as follows:
```sql
create filter ccl(
        desc='The description of the filter ccl',
        priority='1000',
        status='ACTIVE'
)with_pattern(
        plans='Update,Delete',
        fully_qualified_table_names='d1.t1'
)execute(
        action='CONCURRENCY_CONTROL',
        action_args='max_queue_size=100; max_concurrency=10'
);
```
The above filter will control the concurrency of the SQL queries that match the pattern. The maximum number of concurrent queries allowed is 10, and the maximum number of queries allowed in the queue is 100.
