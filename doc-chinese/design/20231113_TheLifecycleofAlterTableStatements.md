---
标题: Alter Table 语句的生命周期
---

# **深入理解 Vitess Online DDL：Alter Table 语句的生命周期**

**Online DDL 中 Alter Table 的生命周期**

1. DDL SQL 是如何解析的。

   在 DDL SQL 发送到 Vtgate 之后，它会经过一个解析器生成执行计划。通常有两种类型的计划：normalDDL 和 onlineDDL。normalDDL 直接在 MySQL 实例上执行。

2. onlineDDL 计划是如何创建的。

   当设置了 **`ddl_strategy='vitess'`** 或 **`ddl_strategy='online'`** 时，vtgate 解析 SQL 以提取 onlineDDL 所需的信息，并为该 onlineDDL 生成一个唯一的 uuid。原始语句 **`alter table t1 add column v3 int;`** 被重写为 **`alter /*vt+ uuid="..." context="..." tableSchema="..." table="..." strategy="..." options="" */ table test.t1 add column v3 int`**。这种格式有助于 vttablet 在接收到 SQL 后进行解析并获取相关信息，然后将其发送到主 Tablet。

3. 将 DDL 计划插入到 **`schema_migrations`** 表中的过程。

   在 vttablet 端，SQL 被解析为 onlineDDL 对象并进行 DDL 冲突验证后，插入到 **`mysql.schema_migrations`** 表中。**`mysql.schema_migrations`** 表中的条目可以看作是 onlineDDL 任务，其初始状态为 **`queued`**（排队）。

   onlineDDL 的状态包括 **`queued`**（排队）、**`ready`**（准备）、**`running`**（运行中）、**`complete`**（完成）、**`fail`**（失败）、**`cancel`**（取消）、**`pause`**（暂停）：

    - **`queued`**（排队）: 新创建的 onlineDDL，初始状态。
    - **`ready`**（准备）: onlineDDL 已准备好执行。
    - **`running`**（运行中）: onlineDDL 正在进行中，包括创建影子表、生成 select 和 insert 语句以及启动 Vreplication 任务。
    - **`complete`**（完成）: onlineDDL 执行完毕，在切换完成后进入此状态。
    - **`fail`**（失败）: 由于内部错误导致执行失败。可以通过使用相应 uuid 查询 **`message`** 字段来诊断错误详情。
    - **`cancel`**（取消）: WeSQL 提供 **`alter schema_migration`** 命令来取消Online DDL 任务。处于此状态的任务可以使用 **`retry`** 命令重新执行。
    - **`pause`**（暂停）: 目前正在开发中，此功能可以暂停处于 **`queue`**、**`ready`** 或 **`running`** 状态的Online DDL 任务。

   有关状态转换，请参阅文章 [OnlineDDLScheduler](20231113_OnlineDDLScheduler.md)。

   对于 alter 语句，onlineDDL 在任务发出后主要执行三个操作：

    1. **创建影子表（Scheduler）**
        - 此任务由主 tablet 中的调度器执行。
        - 它涉及设置一个影子表，该表最终将替换原始表。
    2. **复制数据（Vreplication）**
        - 由一个称为 Vreplication 的独立组件处理。
        - 该组件负责从原始表到影子表的数据全量和增量复制，详情请参考文章 [Vreplication](20231113_Vreplication.md)。
    3. **源表与影子表之间的切换（Cutover）**
        - 当影子表与源表之间的差异低于指定阈值时，会进行这一步操作。
        - 此时，源表被锁定以防止数据操作语言（DML）操作，确保数据一致性。
        - 系统等待影子表赶上源表。
        - 一旦同步完成，切换操作将影子表变为新的源表，完成 alter 过程。