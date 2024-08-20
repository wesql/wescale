---
标题: 透明failover
---

# 引言

failover是一个用来确保当原始数据库实例不可用时，能够用另一个实例替换以保持高可用性的功能。多种因素可能触发failover事件，包括数据库实例故障或计划中的维护程序，如数据库升级。

在没有 WeSQL WeScale 的情况下，failover需要短暂的停机时间。现有的数据库连接会被断开，您的应用程序需要重新打开这些连接。WeSQL WeScale 能够自动检测到failover，并在其内存中缓冲应用程序的 SQL，同时保持应用程序连接的完整性，从而增强在数据库故障情况下的应用程序弹性。

本教程的目标是解释如何启用 WeSQL WeScale 的“透明failover”功能。

# 通过启动参数进行设置

```bash
vtgate \
    # 启用透明failover
    --enable_buffer
    --buffer_size 10000
    --buffer_window 30s
    --buffer_max_failover_duration 60s
    --buffer_min_time_between_failovers 60s
    # 其他必要的命令行选项
    ...
```

# 参数解释

| 参数                                | 说明                                                                                                                                                                          |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enable_buffer                     | 启用缓冲功能。默认未启用                                                                                                                                                                |
| buffer_size                       | 默认值：10000。应根据缓冲事件期间预期的请求数量来调整大小。通常，如果每个连接有一个请求，那么每个连接将占用 buffer_size 的一个缓冲槽，并在 vtgate 上处于“暂停”状态。设置此标志时需要考虑内存资源。                                                             |
| buffer_window                     | 默认值：10秒。每个单独请求应缓冲的最长时间。应小于 `buffer_max_failover_duration` 的值，并根据应用程序需求进行调整。需要注意的是，如果您的 MySQL 客户端设置了 `write_timeout` 或 `read_timeout`，这些值应大于 `buffer_max_failover_duration`。 |
| buffer_max_failover_duration      | 默认值：20秒。如果缓冲时间超过此设定的时长，停止缓冲并向客户端返回错误。                                                                                                                                       |
| buffer_min_time_between_failovers | 默认值：1分钟。如果同一个分片在此时长内连续发生failover，不再进行缓冲。此设置的目的是避免连续的failover事件，在这种情况下，vtgate 可能会缓冲但从未清空缓冲区。                                                                                         |

最重要的两个参数是 `buffer_window` 和 `buffer_max_failover_duration`。

`buffer_window` 控制每条 SQL 最长可以缓冲的时间。

`buffer_max_failover_duration` 控制“缓冲功能”最多可以持续的时间。

如果这两个参数中的任意一个超时，相应的 SQL 将被从缓冲区取出并执行。