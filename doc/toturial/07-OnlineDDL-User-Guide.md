# **onlineDDL User Guide**

## **Introduction**

WeSQL revolutionizes online DDL processes in MySQL by offering a non-blocking, asynchronous, and scheduled approach, simplifying schema migrations. Here are the key distinctions:

**Non-Blocking and Asynchronous**: Unlike traditional MySQL DDL operations that block processes either on the primary or replicas and consume extensive resources, WeSQL's online DDL is designed to be non-blocking and asynchronous.

**Automated Process**: WeSQL reduces manual intervention by automating steps like statement construction, cluster discovery, scheduling, execution, and monitoring progress. This is particularly beneficial for large deployments with numerous shards and clusters.

**Simplified Schema Migration**: By allowing standard ALTER TABLE statements, WeSQL makes the user interface more intuitive, simplifying the schema migration process.

**Resource Management and Recovery**: WeSQL handles resource usage more gracefully and offers better mechanisms for interruption and throttling. It also automates the handling of artifacts from migration tools and improves failure recovery processes.
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
- allow-zero-in-date: Normally, WeSQL runs in strict sql_mode. If you have columns like my_datetime DATETIME DEFAULT '0000-00-00 00:00:00' and want to run DDL on these tables, WeSQL would block the migration due to invalid values. The --allow-zero-in-date option allows using zero-date or zero-in-date.
- declarative: Declarative migration, performed using create and drop operations, further details can be found in another article (todo: write an article about declarative).
- fast-range-rotation: The table partitioning feature has been removed.
- in-order-completion: Completes migrations in order. If migrations queued before the current one are still in pending status (queued, ready, running), then the current migration will have to wait until previous ones are completed. This flag is mainly to support multiple online DDLs running concurrently in an orderly manner.
    - This feature is designed to allow users to initiate multiple migrations simultaneously, which may depend on each other. For instance, task2 depends on task1, so they must be executed in the order they were submitted to achieve correct results.
    - Applicable to **`CREATE|DROP TABLE|VIEW`** statements, and also supports **`alter table`** in **`vitess|online`** mode.
    - **`alter table`** is not applicable for **`mysql`**, or **`direct`** modes.
- postpone-completion: Only transitions state under user command, not automatically completed, allowing better control over the timing of schema changes.
- prefer-instant-ddl: Prioritize using the instant-ddl function of mysql. [instant-ddl](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html)

If a migration is both **`--declarative`** and **`--postpone-completion`**, it will remain in **`queued`** status until the user executes the **`alter vitess_migration ... complete`** command.

- postpone-launch: Initializes a queue in **`queued`** status, set to transition state only on user command.
- singleton: Allows only one migration in pending status to be submitted at a time. From the point a migration enters **`queued`** status, until it reaches **`completion`**, **`failure`**, or **`cancellation`**, no other migrations can be submitted. New migrations will be rejected. This can be seen as a mutex lock for migrations, affecting only those with the **`singleton`** flag, others are not impacted.
- singleton-context: Only allows migrations submitted in the same context to be in **`pending`** status at any given time. Migrations submitted in different contexts will be rejected as long as at least one of the initially submitted migrations is still pending.

Note: It does not make sense to combine **`--singleton`** and **`--singleton-context`**.

Usage:

```sql
set @@ddl_strategy='online --allow-concurrent';
```

## **Monitoring DDL Progress**

After executing onlineDDL, a uuid is returned. You can use this uuid and the **`show vitess_migrations`** command to:

- View the ddl status

```sql
show vitess_migrations like 'uuid' \G;
```

## ****Controlling**** ddl

- Cancel a ddl

```sql
alter vitess_migration 'uuid' cancel;
```

- Cancel all ddl

```sql
alter vitess_migrations cancel all;
```

This command will cancel all ddl in pending (queued, ready, running) status.

- Advancing ddl in postpone status

```sql
alter vitess_migrations 'uuid' running; queued -> running
alter vitess_migrations 'uuid' complete; running -> complete
```

Under the effect of **`--postpone`**, transitioning from **`running -> complete`** is actually the process of manually performing a cutover.