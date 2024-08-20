---
标题: 标题读写分离与负载均衡
---

# 介绍

WeSQL WeScale 通过自动将读查询路由到只读节点，将写查询路由到读写节点，从而简化了应用程序的逻辑。这是通过解析和分析 SQL 语句来实现的，这不仅改善了负载均衡，还确保了资源的高效利用。

WeSQL WeScale 还通过使用多种负载均衡策略将查询路由到合适的节点，帮助管理只读节点。这确保了工作负载在所有可用节点之间均匀分布，从而优化了性能和资源利用率。

本教程的目的是解释如何启用 WeSQL WeScale 的读写分离和负载均衡功能。

# 通过 set 命令进行设置

如果您已经设置好了集群并希望启用读写分离和负载均衡，最简单的方法是使用 MySQL 的 "set" 命令：

```
# 会话级别
set session read_write_splitting_policy='random';

# 全局级别
set global read_write_splitting_policy='random';

```

目前，WeSQL WeScale 支持以下负载均衡策略：

| 策略                   | 说明                                                                                                                    |
|----------------------|-----------------------------------------------------------------------------------------------------------------------|
| disable              | 不启用读写分离                                                                                                               |
| random               | 随机分配读流量                                                                                                               |
| least_qps            | WeSQL WeScale 会在内存中记录发送到后端 MySQL 的 QPS，并将请求重定向到 QPS 最低的 MySQL。                                                        |
| least_global_qps     | WeSQL WeScale 会定期拉取所有 MySQL 的 QPS，并将请求重定向到 QPS 最低的 MySQL。与 least_qps 的区别在于，least_global_qps 从后端 MySQL 获取 QPS，而不是自己追踪。 |
| least_rt             | WeSQL WeScale 会记录所有 SQL 查询的执行时间，并将请求重定向到响应时间（RT）最低的 MySQL。                                                            |
| least_behind_primary | WeSQL WeScale 会定期拉取所有 MySQL 的 GTIDs，并将请求重定向到 GTID 最前沿的 MySQL。                                                         |

# 通过启动参数进行设置

如果您需要设置 `read_write_splitting_policy` 的默认值，可以将其作为 `vtgate` 进程的启动参数传递：

```
vtgate \
    # 启用读写分离和负载均衡
    --read_write_splitting_policy random
    # 其他必要的命令行选项
    ...

```

# 读写分离的转发规则

- 仅发送到主实例
    - INSERT、UPDATE、DELETE、SELECT FOR UPDATE。
    - 所有 DDL 操作（创建/删除表/数据库、修改表结构、权限等）。
    - 所有事务内的请求，除非该事务为只读且 `enable_read_write_splitting_for_read_only_txn=true`。
    - 使用临时表的请求。
    - 使用 GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS/IS_FREE_LOCK 的请求。
    - SELECT last_insert_id() 语句。
    - 所有 SHOW 命令（最好指定具体的节点）。
    - information_schema、performance_schema、mysql、sys 数据库。
    - LOAD DATA 语句。
- 发送到只读或主实例
    - 事务外的 SELECT 语句。
    - COM_STMT_EXECUTE 命令。
    - 在只读事务内的有效请求且 `enable_read_write_splitting_for_read_only_txn=true`。
- 仅发送到 VTGate 层
    - 所有对用户变量的查询和修改。
    - USE 命令。
    - COM_STMT_PREPARE 命令。

您可以使用 `set session/global enable_display_sql_execution_vttablets=true` 命令详细查看 SQL 的完整路由过程。

该过程主要包括两个步骤：1. 确定 tablet 类型。2. 在步骤 1 中确定的类型的 tablets 中，根据负载均衡规则选择一个 tablet 来执行 SQL。

在步骤 1 中，用户可以使用 `use database@tablet_type` 来指定 keyspace tablet 类型。例如，使用 `use mydb@REPLICA` 将导致所有后续的 SQL 默认指向 REPLICA 类型的对应 tablets。

用户也可以使用提示来指定当前 SQL 应该发送到哪个 tablet。例如，`select /*vt+ ROLE=PRIMARY*/ * from mytable;` 将导致 SQL 在主 tablets 上执行。

如果用户没有指定 keyspace tablet 类型和提示，读写分离模块也会根据规则解析出建议的 tablet 类型。

**它们之间的优先级关系是：用户提示的 tablet 类型 > keyspace tablet 类型 > 建议的 tablet 类型**。

在使用上述 set 命令后，您可能会看到如下内容：
```
mysql> select * from t;
+------+------+
| c1   | c2   |
+------+------+
|    2 |    2 |
| 2222 | 2222 |
+------+------+
2 rows in set (0.01 sec)
the sql is executed on zone1-101(PRIMARY)
suggested vttablet type is PRIMARY, read write splitting ratio is 100%, read write splitting policy is DISABLE, sql should execute on PRIMARY
load balance policy is RANDOM, load balance between PRIMARY vttablets
```

# 将只读事务路由到只读节点

当启用了读写分离后，您可以在客户端使用 "set" 命令 `set session enable_read_write_splitting_for_read_only_txn=true;` 或 `set global enable_read_write_splitting_for_read_only_txn=true;` 来启用只读事务路由。

启用此功能后，只读事务中的 SQL 语句将被路由到只读节点执行，这可以减少主节点的负载。

默认情况下，只读事务路由功能是禁用的，这意味着只读事务将像所有其他事务一样被路由到主节点。启用此功能后，您可以使用 "set" 命令 `set session enable_read_write_splitting_for_read_only_txn=false;` 或 `set global enable_read_write_splitting_for_read_only_txn=false;` 来禁用它。

请注意，您也可以在只读事务中执行 "set" 命令来打开或关闭此功能，但该开关不会立即生效，它将在当前只读事务结束后生效。

用户可以使用以下方法启动只读事务：
```
start transaction read only;
start transaction read only, with consistent snapshot;
```

还需要注意的是，在启用只读事务后，如果用户使用以下 SQL：
```
// select last_insert_id() 是一个特殊情况，它不是一个只读查询
if sqlparser.ContainsLastInsertIDStatement(s) {
    return false, nil
}
// GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS 是一个特殊情况，它不是一个只读查询
if sqlparser.ContainsLockStatement(s) {
    return false, nil
}
// 如果涉及系统表
if hasSystemTable(s, "") {
    return false, nil
}
```
这些 SQL 按理说应该路由到主节点，但 WeScale 不会报错，而是强制它们路由到只读节点执行，这可能会导致未定义的结果。