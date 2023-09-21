# Introduction

When OnlineDDL mode is enabled, the Drop Table statement will not immediately delete the table from the database. WeScale will take charge of this process, ensuring that the Drop Table statement does not have any negative performance impact on the database.

Additionally, the table that is about to be deleted will be retained for a period of time, providing users with an option to revert the action if needed.

# Problems with DROP TABLE

Doing a direct `DROP TABLE my_table` in production can be a risky operation. In busy environments, this can result in a significant database lock, lasting from seconds to minutes or even longer.

There are two aspects of locking when it comes to dropping tables:

- Purging the dropped table's pages from the InnoDB buffer pool(s).
- Removing the table's data file (`.ibd`) from the filesystem.

The specific locking behavior and duration may vary depending on various factors:

- The filesystem in use.
- Whether the MySQL adaptive hash index is utilized.
- Whether you are attempting to work around some of the performance issues with MySQL's `DROP TABLE` by using hard links.

It is a widely accepted practice to avoid direct `DROP TABLE` statements and instead follow a more comprehensive table lifecycle.

`In MySQL 8.0.23, the issues with DROP TABLE have been addressed. At WeScale, our course of action is determined by the MySQL version. See below for more details.`

# Table Lifecycle

The lifecycle provided by WeScale includes the following stages or a portion of them:

> in use -> hold -> purge -> evac -> drop -> removed
>

To gain a better understanding of the process, please refer to the following breakdown:

- `In use`: the table is being used and serving traffic, just like a regular table.
- `hold`: the table is given a new arbitrary name. The application cannot see it and thinks it's gone. But actually, the table still exists with all its data intact. We can bring it back (for example, if we realize that some application still needs it) by renaming it back to its original name.
- `purge`: the table is currently being cleared (i.e. rows are being deleted). The clearing process is finished when the table is completely empty. Once the clearing process is done, the table no longer contains any pages in the buffer pool(s). However, during the clearing process, the table pages are loaded into cache in order to remove rows. WeScale clears the table a few rows at a time and uses a throttling mechanism to reduce the load. WeScale disables binary logging for the clearing process. The deletions are not recorded in the binary logs and are not replicated. This lowers the load on disk IO, network, and replication delay. The data is not cleared on the replicas. Experience has shown that removing a table with data on a replica has a smaller impact on performance compared to the primary, and the tradeoff is worth it.
- `evac`: a chill time where we expect the regular production traffic to slowly peace out from the (now inactive) table's pages in the buffer pool. WeScale has this period hardcoded for a solid `72` hours. It's a rough estimate, we don't keep track of table pages in the buffer pool.
- `drop`: an actual `DROP TABLE` is about to go down
- *removed*: table is dropped. When using InnoDB and `innodb_file_per_table` this means the `.ibd` data file backing the table is taken out, and disk space is freed up.

In MySQL **8.0.23** and later, when you drop a table, it won't lock the InnoDB buffer pool and won't block queries that don't use the table being dropped. With WeScale, it will automatically check if the MySQL server version is 8.0.23 or later and will:

- Skip the `purge` stage, even if it's defined
- Skip the `evac` stage, even if it's defined

# OnlineDDL Workflow

- Before VTGate sending `DROP TABLE` statement to VTTablet, it will parse and analyze the statement. A multi-table `DROP TABLE` statement is converted to multiple single-table `DROP TABLE` statements. The code is located at: `go/vt/schema/online_ddl.go#NewOnlineDDLs()` .
- `DROP TABLE` task is executed in VTTablet according to the OnlineDDL task flow. The code is located at: `go/vt/vttablet/onlineddl/executor.go#onMigrationCheckTick()` .
- `DROP TABLE` statement can then be converted to a `RENAME TABLE` statement and executed. The code is located at: `go/vt/vttablet/onlineddl/executor.go#executeDropDDLActionMigration()` .
- Then, the table to be deleted enters the Table Lifecycle, where it will be managed by a scheduled task to modify its status, delete data, until it is finally removed. The code is located at: `go/vt/vttablet/tabletserver/gc/tablegc.go#operate()` .