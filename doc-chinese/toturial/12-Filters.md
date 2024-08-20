---
题目: Filters
---

# Filters

## 介绍

Filter模式通常用于API网关中，用来拦截和修改请求和响应。用户可以定义一组规则来筛选出需要的请求，并在请求到达后端服务之前或响应返回之后对其进行修改。

WeScale 作为一个对开发者友好的数据库代理，提供了类似的功能，让用户可以筛选出需要的 SQL 查询，并在查询执行前或执行后对其进行修改。

关于设计的详细信息，请参阅[设计文档](..%2Fdesign%2F20240514_Filters.md)。

## 创建filter

创建filter的语法简单明了。例如，要创建一个名为 `foo` 的filter，可以使用以下 SQL 命令：
```sql
create filter foo(
        desc='filter foo 的描述',
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

我们来分解一下上面的命令：
- `create filter foo`：这个命令创建了一个名为 `foo` 的filter。
- filter定义的第一部分是filter的元数据部分：
    - `desc='filter foo 的描述'`：这是filter的描述。
    - `priority='1000'`：这是filter的优先级，值越小，优先级越高。
    - `status='ACTIVE'`：这是filter的状态，可以是 `ACTIVE`（激活）或 `INACTIVE`（未激活）。
- filter定义的第二部分是模式匹配部分：
    - `plans='Select,Insert'`：这是filter将匹配的 SQL 查询类型，可以是 `Select`、`Insert`、`Update`、`Delete`。
    - `fully_qualified_table_names='d1.t1'`：这是filter将匹配的完整表名。
- filter定义的第三部分是操作部分：
    - `action='FAIL'`：这是当匹配成功时filter将执行的操作。`FAIL` 表示当filter匹配成功时，SQL 查询将失败。

## `create filter` 语句的完整语法

`create filter` 语句的完整语法如下：
```sql
create filter [if not exists] $name(
        desc='',
        priority='1000', -- 默认 1000
        status='ACTIVE' -- ACTIVE 或 INACTIVE，默认 ACTIVE
)
with_pattern(
        plans='Select,Insert', -- Select, Insert, Update, Delete
        fully_qualified_table_names='',
        query_regex='', -- 匹配查询的正则表达式
        query_template='', -- 用于匹配查询的查询模板，例如 select * from t1 where id = :id
        request_ip_regex='', -- 匹配请求IP的正则表达式
        user_regex='', -- 匹配用户的正则表达式
        leading_comment_regex='', -- 匹配查询前置注释的正则表达式
        trailing_comment_regex='' -- 匹配查询后置注释的正则表达式
)
execute(
        action='FAIL', -- 当filter匹配时执行的操作，可能的值：CONTINUE、FAIL、CONCURRENCY_CONTROL
        action_args='' -- 操作的参数
);
```

## 修改filter

要修改filter，可以使用 `alter filter` 语句。例如，要更改filter `foo` 的描述，可以使用以下 SQL 命令：
```sql
alter filter foo(
        desc='filter foo 的新描述',
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

## 显示filter

要显示所有filter，可以使用 `show filters` 语句。例如：
```sql
mysql> show filters\G
*************************** 1. row ***************************
                         id: 4
           create_timestamp: 2024-05-17 15:29:31
           update_timestamp: 2024-05-17 15:29:31
                       name: foo
                description: filter foo 的描述
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

你也可以使用 `show create filter` 来显示filter的定义。
```sql
mysql> show create filter foo\G;
*************************** 1. row ***************************
        Filer: foo
Create Filter: create filter foo ( desc='filter foo 的描述', priority='1000', status='ACTIVE' ) with_pattern ( plans='Select,Insert', fully_qualified_table_names='d1.t1', query_regex='', query_template='', request_ip_regex='', user_regex='', leading_comment_regex='', trailing_comment_regex='', bind_var_conds='' ) execute ( action='FAIL', action_args='' )
1 row in set (0.00 sec)
```

## 解释filter

对于特定的 SQL 查询，你可以使用 `/*explain filter*/` 前置注释来解释哪个filter匹配了查询。例如：
```sql
mysql> /*explain filter*/select * from t1;
+------+-----------------------------------+----------+--------+-------------+
| Name | description                       | priority | action | action_args |
+------+-----------------------------------+----------+--------+-------------+
| foo  | filter foo 的描述                  | 1000     | FAIL   |             |
+------+-----------------------------------+----------+--------+-------------+
1 row in set (0.00 sec)


mysql> select * from t1;
ERROR 1105 (HY000): target: d1.0.primary: vttablet: rpc error: code = InvalidArgument desc = disallowed due to rule: foo (CallerID: userData1)
```

## 删除filter

要删除filter，可以使用 `drop filter` 语句。例如，要删除filter `foo`，可以使用以下 SQL 命令：
```sql
drop filter foo;
```

## 操作

WeScale 当前支持以下内置操作：
- `CONTINUE`：继续执行 SQL 查询。无需参数。
- `FAIL`：使 SQL 查询失败。无需参数。
- `CONCURRENCY_CONTROL`：控制 SQL 查询的并发性。它接受以下参数：
    - `max_concurrency`：允许的最大并发查询数。
    - `max_queue_size`：队列中允许的最大查询数，包括正在执行的查询。

使用 `CONCURRENCY_CONTROL` 操作的示例如下：
```sql
create filter ccl(
        desc='filter ccl 的描述',
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
上述filter将控制匹配模式的 SQL 查询的并发性。允许的最大并发查询数为 10，队列中允许的最大查询数为 100。