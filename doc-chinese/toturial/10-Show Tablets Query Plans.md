---
title: 查询 Tablets 中的执行计划
---

## 使用场景

在 WeScale 中，当用户发送 SQL 命令到 vtgate 时，vtgate 会将其路由到对应的 vttablets，而 vttablets 最终会将该命令发送到 MySQL 执行。vttablets 会缓存最近执行的 SQL 计划并记录它们的执行状态。

用户可以使用 `show tablets_plans` 命令获取最近执行的 SQL 命令的执行信息，这有助于排查应用程序中的性能问题。

## 使用方法

1. 要获取所有 vttablets 的计划缓存信息，可以使用 `show tablets_plans` 命令。
```sql
show tablets_plans
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
| tablet_alias | query_template                                              | plan_type | tables       | query_count | accumulated_time | accumulated_mysql_time | rows_affected | rows_returned | error_count |
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
| zone1-101    | insert into mydb.mytable(`name`, age) values (:vtg1, :vtg2) | Insert    | mydb.mytable | 5           | 9.556459ms       | 5.012709ms             | 5             | 0             | 0           |
| zone1-101    | select * from mydb.mytable                                  | Select    | mydb.mytable | 2           | 1.950542ms       | 533.916µs              | 0             | 9             | 0           |
| zone1-101    | explain mytable                                             | OtherRead |              | 1           | 1.237125ms       | 1.212084ms             | 0             | 3             | 0           |
| zone1-100    | select * from mydb.mytable                                  | Select    | mydb.mytable | 3           | 5.952293ms       | 1.582958ms             | 0             | 15            | 0           |
| zone1-102    | select * from mydb.mytable                                  | Select    | mydb.mytable | 3           | 3.283624ms       | 913.208µs              | 0             | 15            | 0           |
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
5 rows in set (0.00 sec)
```

- `tablet_alias`：tablet 的别名。
- `query_template`：SQL 的模板，具有相同模板的 SQL 会命中相同的计划。
- `plan_type`：计划的类型。
- `tables`：SQL 涉及的数据库和表。
- `query_count`：计划命中的次数。
- `accumulated_time`：处理该计划在 vttablet 和 MySQL 上累计花费的时间。
- `accumulated_mysql_time`：处理该计划在 MySQL 上累计花费的时间。
- `rows_affected`：该计划影响的总行数。
- `rows_returned`：该计划返回的总行数。
- `error_count`：该计划遇到的总错误次数。

2. 要获取特定 vttablet 的计划缓存信息，可以使用 `show tablets_plans like 'tablet_alias'` 或 `show tablets_plans alias='tablet_alias'`。

`like` 可以与通配符一起使用，例如 `Show tablets_plans like '%102'`。

需要注意的是，如果您添加了一个 WeScale Filter，缓存中的计划将被清除。