---
标题: OnlineDDL模式下DROP TABLE的执行过程
---

# 引言

当启用OnlineDDL模式时，Drop Table语句不会立即从数据库中删除表。WeScale将负责这一过程，确保Drop Table语句对数据库的性能影响尽可能小。

此外，即将删除的表将在一段时间内被保留，提供用户在必要时撤销该操作的选项。

# DROP TABLE相关问题

在生产数据库中执行 `DROP TABLE my_table` 可能是一项风险操作。在负载环境下，这可能会导致严重的数据库锁定，持续时间从几秒到几分钟甚至更长。

删除表时有两种类型的锁：

- 从InnoDB缓冲池中清除表的页。
- 从文件系统中移除表的数据文件 (`.ibd`)。

具体的锁定行为和持续时间可能因多种因素而异：

- 使用的文件系统。
- 是否使用MySQL的自适应哈希索引。
- 是否尝试通过使用硬链接来规避MySQL `DROP TABLE` 的一些性能问题。

通常建议避免直接执行 `DROP TABLE` 语句，而是执行一系列更细致的步骤，以逐步维护表删除生命周期，如下所示。

`在MySQL 8.0.23中，DROP TABLE的问题已经得到解决。在WeScale，我们的行动策略取决于MySQL版本。详情请参见下文。`

# 表删除的生命周期

在WeScale中执行 `DROP TABLE` 期间，表的状态按以下阶段顺序变化：

> in use -> hold -> purge -> evac -> drop -> removed
>

每个阶段的含义如下：

- `In use`: 表正在使用中，像普通表一样提供流量服务。
- `hold`: 表被重命名为一个新的临时表。应用程序无法再看到它，并认为它已经消失。但实际上，表仍然存在，所有数据都完好无损。如果发现某些应用程序仍然需要它，我们可以通过将其重命名回原来的名称来恢复它。
- `purge`: 临时表中的行正在被删除。清理过程在表完全为空时结束。清理过程完成后，表不再包含缓冲池中的任何页。然而，在清理过程中，表页会被加载到缓存中以删除行。WeScale一次只从临时表中删除一小批行，并使用节流机制来限制删除操作引起的额外负载。WeScale禁用了清理过程中的二进制日志记录。这些删除操作不会记录在二进制日志中，也不会被复制。这降低了磁盘IO、网络和复制延迟的负载。数据不会在副本上被清除。经验表明，在副本上删除带有数据的表对性能的影响比在主库上小，经过权衡后，这是值得的。
- `evac`: 一个冷却期，我们期望常规生产流量逐渐从（现在处于非活动状态的）表页中消失。WeScale将此期间硬编码为整整 `72` 小时。这是一个粗略估计，我们不会跟踪缓冲池中的表页。
- `drop`: 实际的 `DROP TABLE` 操作即将执行。
- *removed*: 表已被删除。当使用InnoDB和`innodb_file_per_table`时，这意味着支持表的 `.ibd` 数据文件被移除，磁盘空间被释放。

在MySQL **8.0.23**及之后的版本中，当你删除表时，它不会锁定InnoDB缓冲池，也不会阻塞那些没有使用被删除表的查询。使用WeScale时，它会自动检查MySQL服务器版本是否为8.0.23或更高，并且会：

- 跳过 `purge` 阶段，即使已定义该阶段。
- 跳过 `evac` 阶段，即使已定义该阶段。

# OnlineDDL工作流程

- 在VTGate将 `DROP TABLE` 语句发送到VTTablet之前，它将解析并分析该语句。多表 `DROP TABLE` 语句将转换为多个单表 `DROP TABLE` 语句。代码位于：`go/vt/schema/online_ddl.go#NewOnlineDDLs()` 。
- `DROP TABLE` 任务在VTTablet中根据OnlineDDL任务流程执行。代码位于：`go/vt/vttablet/onlineddl/executor.go#onMigrationCheckTick()` 。
- `DROP TABLE` 语句可以转换为 `RENAME TABLE` 语句并执行。代码位于：`go/vt/vttablet/onlineddl/executor.go#executeDropDDLActionMigration()` 。
- 然后，要删除的表进入表删除生命周期，其中它将由计划任务管理，逐步修改其状态，分批删除行，直到最终被移除。代码位于：`go/vt/vttablet/tabletserver/gc/tablegc.go#operate()` 。
