---
标题: 最少繁忙连接负载均衡策略
---

- 功能名称:
- 开始日期: 2023-08-09
- 作者: lyq10085
- 问题链接: https://github.com/WeSQL/WeScale/issues/220
- PR链接: https://github.com/WeSQL/WeScale/pull/230

# 摘要

该提案旨在为 vtgate 的现有负载均衡器添加新策略（LEAST_MYSQL_CONNECTED_CONNECTIONS, LEAST_MYSQL_RUNNING_CONNECTIONS, LEAST_TABLET_INUSE_CONNECTIONS）。新策略将使 vtgate 能够选择根据vttablet的连接池使用情况进行负载均衡。

# 动机

在实践中，mysqld 中的连接数量反映了 vttablet 和 mysqld 进程的负载。基于 MySQL 连接数的路由请求的负载均衡策略将提高 WeSQL WeScale 集群的 QPS（每秒查询数）。

# 技术设计

## 目标

1. LEAST_MYSQL_CONNECTED_CONNECTIONS: vtgate 根据 MySQL 的已连接数路由查询。
2. LEAST_MYSQL_RUNNING_CONNECTIONS: vtgate 根据 MySQL 的运行中连接数路由查询。
3. LEAST_TABLET_INUSE_CONNECTIONS: vtgate 根据 vttablet 连接池的使用情况路由查询。

## 设计细节

1. vttablet 的 tabletserver 使用 healthstreamer 定期收集各个连接池的使用情况以及底层 mysqld 的连接状态，并将这些状态包装为 streamhealthreponse 的子字段，通过 grpc 进行流式传输。
2. vtgate 的 tabletgateway 通过 streamHealth grpc 获取 vttablets 的连接池使用情况，并将其缓存到 tabletgateway 的健康检查数据中。
3. vtgate 的查询执行器根据 vtgate 的 tabletgateway 缓存的健康检查数据选择合适的 tablet。

## 路线图

- [x] 实现 statemanager 的 replTracker 的 ThreadsStatus 方法。

- [x] 实现 queryEngine 和 TxEngine 的 getConnPoolInUseStats 方法。

- [x] 在 statemanager 的广播方法中添加更新 heathstreamer 的 RealTimeStats 逻辑。

- [x] 实现 tabletgateway 负载均衡器的 leastMysqlConnectedConnections、leastMysqlRunningConnections、leastTabletInUseConnections 方法。

# 用法

```MySQL
set @@read_write_splitting_policy='least_mysql_connected_connections';
set @@read_write_splitting_policy='least_mysql_running_connections';
set @@read_write_splitting_policy='least_tablet_inuse_connections';
```

# 未来工作

使 vtgate 能够主动检查 vttablet 连接池的使用情况。

# 参考文献

[WeSQL WeScale 负载均衡器](https://github.com/WeSQL/WeScale/issues/64)