---
title: OnlineDDL User Guide
---

# **onlineDDL User Guide**

## **Introduction**

WeSQL WeSCale revolutionizes online DDL processes in MySQL by offering a non-blocking, asynchronous, and scheduled approach, simplifying schema migrations. Here are the key distinctions:

**Non-Blocking and Asynchronous**: Unlike traditional MySQL DDL operations that block processes either on the primary or replicas and consume extensive resources, WeSQL's online DDL is designed to be non-blocking and asynchronous.

**Automated Process**: WeSQL WeScalereduces manual intervention by automating steps like statement construction, cluster discovery, scheduling, execution, and monitoring progress. This is particularly beneficial for large deployments with numerous shards and clusters.

**Simplified Schema Migration**: By allowing standard ALTER TABLE statements, WeSQL WeScalemakes the user interface more intuitive, simplifying the schema migration process.

**Resource Management and Recovery**: WeSQL WeScalehandles resource usage more gracefully and offers better mechanisms for interruption and throttling. It also automates the handling of artifacts from migration tools and improves failure recovery processes.
## **ddl strategy**

The ddl strategy can be set in the session with **`ddl_strategy`**, currently supporting **`online`**, **`mysql`**, **`direct`**.

- online: Uses WeSQL's DDL management strategy to schedule ddl.
- mysql: Utilizes MySQL's own online DDL mechanism, but monitored by WeSQL.
- direct: Directly sends to the backend connected database.

```sql
set @@ddl_strategy='online';
```

## **ddl flag:**

- allow-concurrent: Allows migrations to synchronize with another migration instead of waiting in the queue, but not all migrations can run in parallel, subject to certain restrictions (e.g., simultaneous operations on the same table are not allowed).
- allow-zero-in-date: Normally, WeSQL WeScaleruns in strict sql_mode. If you have columns like my_datetime DATETIME DEFAULT '0000-00-00 00:00:00' and want to run DDL on these tables, WeSQL WeScalewould block the migration due to invalid values. The --allow-zero-in-date option allows using zero-date or zero-in-date.
- declarative: Declarative migration, performed using create and drop operations, further details can be found in another article (todo: write an article about declarative).
- fast-range-rotation: The table partitioning feature has been removed.
- in-order-completion: Completes migrations in order. If migrations queued before the current one are still in pending status (queued, ready, running), then the current migration will have to wait until previous ones are completed. This flag is mainly to support multiple online DDLs running concurrently in an orderly manner.
    - This feature is designed to allow users to initiate multiple migrations simultaneously, which may depend on each other. For instance, task2 depends on task1, so they must be executed in the order they were submitted to achieve correct results.
    - Applicable to **`CREATE|DROP TABLE|VIEW`** statements, and also supports **`alter table`** in **`vitess|online`** mode.
    - **`alter table`** is not applicable for **`mysql`**, or **`direct`** modes.
- postpone-completion: Only transitions state under user command, not automatically completed, allowing better control over the timing of schema changes.
- prefer-instant-ddl: Prioritize using the instant-ddl function of mysql. [instant-ddl](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)

If a migration is both **`--declarative`** and **`--postpone-completion`**, it will remain in **`queued`** status until the user executes the **`alter schema_migration 'uuid' complete`** command.

- postpone-launch: Initializes a queue in **`queued`** status, set to transition state only on user command.
- singleton: Allows only one migration in pending status to be submitted at a time. From the point a migration enters **`queued`** status, until it reaches **`completion`**, **`failure`**, or **`cancellation`**, no other migrations can be submitted. New migrations will be rejected. This can be seen as a mutex lock for migrations, affecting only those with the **`singleton`** flag, others are not impacted.
- singleton-context: Only allows migrations submitted in the same context to be in **`pending`** status at any given time. Migrations submitted in different contexts will be rejected as long as at least one of the initially submitted migrations is still pending.

Note: It does not make sense to combine **`--singleton`** and **`--singleton-context`**.

Usage:

```sql
set @@ddl_strategy='online --allow-concurrent';
```

## **Monitoring DDL Progress**

After executing onlineDDL, a uuid is returned. You can use this uuid and the **`show schema_migration`** command to:

- View the ddl status

```sql
show schema_migration like 'uuid' \G;
```

## ****Controlling**** ddl

- Cancel a ddl

```sql
alter schema_migration 'uuid' cancel;
```

- Cancel all ddl

```sql
alter schema_migration cancel all;
```

This command will cancel all ddl in pending (queued, ready, running) status.

- Pause a ddl
```sql
alter schema_migration 'uuid' pause;
```

This command will pause a ddl in pending (queued, ready, running) status. To guarantee correctness, the subsequent ddls which have the same table with the paused one will be blocked.

- Unpause a ddl
```sql
alter schema_migration 'uuid' resume;
```

- Pause all ddl

```sql
alter schema_migration pause all;
```

- Unpause all ddl

```sql
alter schema_migration resume all;
```

- Advancing ddl in postpone status

```sql
-- queued -> running
alter schema_migration 'uuid' running;
-- running -> complete
alter schema_migration 'uuid' complete; 
```

Under the effect of **`--postpone`**, transitioning from **`running -> complete`** is actually the process of manually performing a cutover.
