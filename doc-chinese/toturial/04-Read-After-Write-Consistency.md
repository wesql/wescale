---
标题: 写后读一致性
---

# 引言

当应用程序向主节点写入数据后，随后在只读节点上读取该数据时，WeSQL WeScale 会确保在主节点上刚刚写入的数据可以被只读节点读取。

本教程的目标是解释如何在 WeSQL WeScale 中启用“写后读一致性”（Read-After-Write-Consistency）功能。

# 通过 set 命令进行设置

如果您已经设置好了集群并希望启用“写后读一致性”，最简单的方法是使用 MySQL 的 "set" 命令：

```
# 会话级别
set session read_after_write_consistency='SESSION'

# 全局级别
set global read_after_write_consistency='SESSION'

```

目前，WeSQL WeScale 支持以下几种一致性级别：

| 策略       | 说明                                                        |
|----------|-----------------------------------------------------------|
| EVENTUAL | 不保证一致性。                                                   |
| SESSION  | 在同一连接中，确保后续的读取请求可以读取到之前的写操作结果。（推荐）                        |
| INSTANCE | 在同一实例（VTGate）中，确保后续的读取请求可以读取到之前的写操作结果，即使读取和写入请求不是由同一连接发起的。 |
| GLOBAL   | 在同一个 WeSQL WeScale 集群中，确保任何读取请求都可以读取到之前的写操作结果。            |

# 通过启动参数进行设置

如果您需要设置 `read_write_splitting_policy` 的默认值，可以将其作为 `vtgate` 进程的启动参数传递：