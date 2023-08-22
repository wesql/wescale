# 

The goal of this tutorial is to explain how to enable read-write-splitting and load balancing functionality for WeSQL-Scale. WeSQL-Scale will parse statements to send write queries to the master server and distribute read queries for load balancing across the slave servers.

# Setting through the set command.

If you have already set up a cluster and want to enable read-write splitting and load balancing, the simplest way is using the MySQL "set" command:

```
# session
set session read_write_splitting_policy='random';

# global
set global read_write_splitting_policy='random';

```

Currently, WeSQL-Scale supports these load balancing policies:

| disable | Do not enable read/write separation |
| --- | --- |
| random | Randomly allocate read traffic |
| least_qps | WeSQL-Scale will record the QPS sent to backend MySQL in its own memory and redirect requests to the MySQL with the lowest QPS. |
| least_global_qps | WeSQL-Scale will periodically pull the QPS of all MySQLs and redirect requests to the MySQL with the lowest QPS. The difference with least_qps is that least_global_qps obtains QPS from the backend MySQL instead of tracking it themselves. |
| least_rt | WeSQL-Scale will record the execution time of all SQL queries and redirect requests to the MySQL with the lowest response time (RT). |
| least_behind_primar | WeSQL-Scale will periodically pull the GTIDs of all MySQLs and redirect requests to the MySQL with the most up-to-date GTID. |

# Setting via launch parameters

If you need to set the default value of read_write_splitting_policy, you can pass it as a startup parameter for the vtgate process:

```
vtgate \
    # enable read-write-splitting and load balancing
    --read_write_splitting_policy random
    # other necessary command line options
    ...

```

# Read-Write-Splitting Forwarding Rules

- Only sent to the primary instance
    - INSERT, UPDATE, DELETE, SELECT FOR UPDATE.
    - All DDL operations (creating/dropping tables/databases, altering table structures, permissions, etc.).
    - All requests within a transaction.
    - Requests that use temporary tables.
    - Requests that use GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS/IS_FREE_LOCK.
    - SELECT last_insert_id() statements.
    - All SHOW commands (preferably specifying a specific node).
    - information_schema, performance_schema, mysql, sys databases.
    - LOAD DATA statements.
- Sent to read-only or primary instances
    - SELECT statements outside of a transaction.
    - COM_STMT_EXECUTE commands.
- Only sent to VTGate layer
    - All queries and modifications to User Variables.
    - USE command.
    - COM_STMT_PREPARE command.