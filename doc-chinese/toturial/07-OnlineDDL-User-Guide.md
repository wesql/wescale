---
标题: OnlineDDL 用户指南
---

# **OnlineDDL 用户指南**

## **简介**

WeSQL WeScale 通过提供一种非阻塞、异步且可调度的方式，彻底改变了 MySQL 中的OnlineDDL 过程，简化了 schema 的迁移。以下是关键区别：

**非阻塞和异步**：与传统的 MySQL DDL 操作不同，后者会在主节点或副本上阻塞进程并消耗大量资源，而 WeSQL WeScale的OnlineDDL 设计为非阻塞和异步的。

**自动化过程**：WeSQL WeScale 通过自动化步骤，如语句构建、集群发现、调度、执行和进度监控，减少了手动干预。这对于拥有众多分片和集群的大规模部署尤其有益。

**简化的架构迁移**：通过允许使用标准的 ALTER TABLE 语句，WeSQL WeScale使用户界面更加直观，简化了 Schema 迁移过程。

**资源管理和恢复**：WeSQL WeScale 更优雅地处理资源使用，并提供了更好的中断和节流机制。它还自动处理迁移工具的产物，并改进了故障恢复过程。

## **ddl 策略**

ddl 策略可以在会话中通过 **`ddl_strategy`** 设置，目前支持 **`online`**、**`mysql`** 和 **`direct`**。

- online: 使用 WeSQL WeScale的 DDL 管理策略来调度 ddl。
- mysql: 利用 MySQL 自身的OnlineDDL 机制，但由 WeSQL WeScale监控。
- direct: 直接发送到连接的后端数据库。

```sql
set @@ddl_strategy='online';
```

## **ddl 标志:**

- allow-concurrent: 允许迁移与另一个迁移同步，而不是在队列中等待，但并非所有迁移都能并行运行，受某些限制（例如，不允许同时对同一张表进行操作）。
- allow-zero-in-date: 通常，WeSQL WeScale在严格模式下运行。如果您有类似 my_datetime DATETIME DEFAULT '0000-00-00 00:00:00' 的列，并且希望在这些表上运行 DDL，WeSQL WeScale会由于无效值阻止迁移。使用 --allow-zero-in-date 选项允许使用全零日期或部分为零的日期。
- declarative: 声明式迁移，使用 create 语句而非 DDL 语句来声明表的目标结构，WeScale 会自动利用目标表和当前表的结构差异去生成 DDL。
- fast-range-rotation: 功能已被移除。
- in-order-completion: 按顺序完成迁移。如果当前迁移之前排队的迁移仍处于待处理状态（排队、准备、运行），则当前迁移必须等待之前的迁移完成。此标志主要支持多个OnlineDDL 以有序方式并行运行。
    - 该功能旨在允许用户同时启动多个可能相互依赖的迁移。例如，task2 依赖于 task1，因此它们必须按提交顺序执行以获得正确的结果。
    - 适用于 **`CREATE|DROP TABLE|VIEW`** 语句，也支持在 **`vitess|online`** 模式下的 **`alter table`** 。
    - 在 **`mysql`** 或 **`direct`** 模式下的 **`alter table`** 语句不适用于此选项。
- postpone-completion: 仅在用户命令下转换状态，不会自动变成完成态，允许更好地控制 schema 更改的时间。
- prefer-instant-ddl: 优先使用 MySQL 的 instant-ddl 功能。[instant-ddl](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)

如果一个迁移同时具有 **`--declarative`** 和 **`--postpone-completion`**，它将保持在 **`queued`** 状态，直到用户执行 **`alter schema_migration 'uuid' complete`** 命令。

- postpone-launch: 初始化一个处于 **`queued`** 状态的队列，仅在用户命令下转换状态。
- singleton: 允许一次只提交一个处于待处理状态的迁移。从迁移进入 **`queued`** 状态，到达到 **`completion`**、**`failure`** 或 **`cancellation`**，不能提交其他迁移。新迁移将被拒绝。这可以看作是迁移的互斥锁，只影响带有 **`singleton`** 标志的迁移，其他迁移不受影响。
- singleton-context: 只允许同一上下文中提交的迁移在任何给定时间处于 **`pending`** 状态。在不同上下文中提交的迁移，如果至少一个最初提交的迁移仍处于待处理状态，则不同上下文提交的迁移将被拒绝。

注意：**`--singleton`** 和 **`--singleton-context`** 组合在一起没有意义。

用法:

```sql
set @@ddl_strategy='online --allow-concurrent';
```

## **监控 DDL 进度**

执行 onlineDDL 后，会返回一个 uuid。您可以使用此 uuid 和 **`show schema_migration`** 命令来：

- 查看 ddl 状态

```sql
show schema_migration like 'uuid' \G;
```

## ****控制**** ddl

- 取消 ddl

```sql
alter schema_migration 'uuid' cancel;
```

- 取消所有 ddl

```sql
alter schema_migration cancel all;
```

此命令将取消所有处于待处理（排队、准备、运行）状态的 ddl。

- 暂停 ddl
```sql
alter schema_migration 'uuid' pause;
```

此命令将暂停处于待处理（排队、准备、运行）状态的 ddl。为了保证正确性，具有与暂停的 ddl 相同表的后续 ddl 将被阻塞。

- 解除暂停 ddl
```sql
alter schema_migration 'uuid' resume;
```

- 暂停所有 ddl

```sql
alter schema_migration pause all;
```

- 解除暂停所有 ddl

```sql
alter schema_migration resume all;
```

- 推进处于延迟状态的 ddl

```sql
-- 排队 -> 运行
alter schema_migration 'uuid' running;
-- 运行 -> 完成
alter schema_migration 'uuid' complete; 
```

在 **`--postpone`** 的作用下，从 **`运行 -> 完成`** 的转换实际上是手动执行切换的过程。