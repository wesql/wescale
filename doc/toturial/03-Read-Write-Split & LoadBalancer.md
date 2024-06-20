# Introduction

WeSQL WeScale simplifies application logic by automatically routing read queries to read-only nodes and write queries to the read-write node. This is achieved by parsing and analyzing SQL statements, which improves load balancing and ensures efficient use of available resources.

WeSQL WeScale also helps manage read-only nodes by routing queries to the appropriate node using various load balancing policies. This ensures that the workload is evenly distributed across all available nodes, optimizing performance and resource utilization.

The goal of this tutorial is to explain how to enable read-write-splitting and load balancing functionality for WeSQL WeScale.

# Setting through the set command.

If you have already set up a cluster and want to enable read-write splitting and load balancing, the simplest way is using the MySQL "set" command:

```
# session
set session read_write_splitting_policy='random';

# global
set global read_write_splitting_policy='random';

```

Currently, WeSQL WeScale supports these load balancing policies:

| disable | Do not enable read/write separation |
| --- | --- |
| random | Randomly allocate read traffic |
| least_qps | WeSQL WeScale will record the QPS sent to backend MySQL in its own memory and redirect requests to the MySQL with the lowest QPS. |
| least_global_qps | WeSQL WeScale will periodically pull the QPS of all MySQLs and redirect requests to the MySQL with the lowest QPS. The difference with least_qps is that least_global_qps obtains QPS from the backend MySQL instead of tracking it themselves. |
| least_rt | WeSQL WeScale will record the execution time of all SQL queries and redirect requests to the MySQL with the lowest response time (RT). |
| least_behind_primar | WeSQL WeScale will periodically pull the GTIDs of all MySQLs and redirect requests to the MySQL with the most up-to-date GTID. |

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
  - All requests within a transaction, except when the transaction is read-only and `enable_read_write_splitting_for_read_only_txn=true`.
  - Requests that use temporary tables.
  - Requests that use GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS/IS_FREE_LOCK.
  - SELECT last_insert_id() statements.
  - All SHOW commands (preferably specifying a specific node).
  - information_schema, performance_schema, mysql, sys databases.
  - LOAD DATA statements.
- Sent to read-only or primary instances
  - SELECT statements outside of a transaction.
  - COM_STMT_EXECUTE commands.
  - Valid requests within a read only transaction and `enable_read_write_splitting_for_read_only_txn=true`.
- Only sent to VTGate layer
  - All queries and modifications to User Variables.
  - USE command.
  - COM_STMT_PREPARE command.

You can use the set command `set session/global enable_display_sql_execution_vttablets=true` to examine the complete routing process of an SQL  in detail. 

The process mainly consists of two steps: 1. Determining the tablet type. 2.Among the tablets with the type determined in step 1, selecting a tablet to execute the SQL based on the load balance rules. 

In step 1, users can use `use database@tablet_type` to specify the keyspace tablet type. For example, using `use mydb@REPLICA` will cause all subsequent SQLs to be directed to the corresponding tablets of REPLICA type by default. 

Users can also use hint to specify which tablet the current SQL should be sent to. For example, `select /*vt+ ROLE=PRIMARY*/ * from mytable;` will cause the SQL to be executed on the primary tablets. 

If the user does not specify keyspace tablet type and hint, the read-write splitting module will also resolve the suggested tablet type according to the rules. 

**The priority relationship between them is: hint tablet type > keyspace tablet type > suggested tablet type**. 

After using the set command mentioned above, you can see something as follows:
```
mysql> select * from t;
+------+------+
| c1   | c2   |
+------+------+
|    2 |    2 |
| 2222 | 2222 |
+------+------+
2 rows in set (0.01 sec)
the sql is executed on zone1-101(PRIMARY)
suggested vttablet type is PRIMARY, read write splitting ratio is 100%, read write splitting policy is DISABLE, sql should execute on PRIMARY
load balance policy is RANDOM, load balance between PRIMARY vttablets
```
 
 # Route Read Only Transaction to Read-Only Nodes
  When read-write splitting is enabled, you can use the "set" command `set session enable_read_write_splitting_for_read_only_txn=true;` or `set global enable_read_write_splitting_for_read_only_txn=true;` on the client side to enable read only transaction routing. 
  
  After enabling this feature, the SQL statements in the read only transaction will be routed to the read only nodes for execution, which can reduce the load on the primary node. 
  
  By default, the read only transaction routing feature is disabled, which means that read only transactions will be routed to the primary node like all other transactions. After enabling this feature, you can use the "set" command `set session enable_read_write_splitting_for_read_only_txn=false;` or `set global enable_read_write_splitting_for_read_only_txn=false;` to disable it. 
  
  Note that you can also execute the "set" command to turn this feature on or off during a read only transaction, but the switch will not take effect immediately. It will take effect only after the current read only transaction ends. 
  
  Users can use the following method to start a read-only transaction:
  ```
start transaction read only;
start transaction read only, with consistent snapshot;
  ```
Also note that after enabling read only transaction, if users use the following SQL:
```
// select last_insert_id() is a special case, it's not a read-only query
if sqlparser.ContainsLastInsertIDStatement(s) {
    return false, nil
}
// GET_LOCK/RELEASE_LOCK/IS_USED_LOCK/RELEASE_ALL_LOCKS is a special case, it's not a read-only query
if sqlparser.ContainsLockStatement(s) {
    return false, nil
}
// if hasSystemTable
if hasSystemTable(s, "") {
    return false, nil
}
```
These SQL should be routed to the primary node by logic, but wescale will not report an error, instead it will force them to be routed to the read-only node for execution, which may result in undefined results.
