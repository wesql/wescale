# Non-Transactional DML

In the Non-Transactional DML tutorial document, we introduced the basic principles and usage of Non-Transactional DML. Building on that, this document delves deeper into the implementation details of Non-Transactional DML from a "developer's perspective." This document will be updated as Non-Transactional DML is optimized.

## Lifecycle of Related Commands
Non-Transactional DML mainly handles three types of requests: submit (users submit a Non-Transactional DML job), show (displaying job-related information to users), and alter (allowing users to control the job).
These three types of commands follow different code paths but ultimately all call the Job Controller's single external interface, `HandleRequest`.

**submit**

When a user wants to submit a DML job, no additional SQL commands are needed; just add a comment to the original DML. During the parsing process of SQL by Wescale vtgate, if the corresponding comment is detected, it invokes the `HandleRequest` function on vttablet via gRPC, thus completing the job submission.

**show**

When a user uses the `show dml_jobs` and `show dml_job 'uuid' [details]` commands, the lexical and syntax parser generates a `ShowDMLJob` AST node. When vtgate recognizes this AST node, it invokes the `HandleRequest` function on vttablet via gRPC to obtain job-related information.

**alter**

When the lexical and syntax parser identifies an alter command related to Non-Transactional DML, it generates an `alterDMLJob` AST node. Unlike the previous two types of commands, Vitess implements better encapsulation for alter commands. After the alter command is sent to vttablet via gRPC, it is routed based on the type of the alter AST node. For the `alterDMLJob` AST node, the `HandleRequest` interface is called, thereby enabling control over the job.

## Supported DML
Only `update` and `delete` DML types are supported.
Only DML with a `where` clause is supported.
Only modifications to a single non-temporary table are supported.
Subqueries are supported in the `where` clause or the `update` set clause.
A table can have multiple primary keys (PK). The types of PKs should be within the following range:
```go
switch pk.pkType {  
case sqltypes.Float64, sqltypes.Float32, sqltypes.Decimal,  
sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary, sqltypes.Bit,  
sqltypes.Text,  
sqltypes.Enum, sqltypes.Set, sqltypes.Tuple, sqltypes.Geometry, sqltypes.TypeJSON, sqltypes.Expression,  
sqltypes.HexNum, sqltypes.HexVal, sqltypes.BitNum:  
return true  
}
```

**Unsupported DML**

If the following DML is submitted as Non-Transactional DML, it may cause unexpected errors:
Modifying the PK value in the `set` clause of an `update` statement. This may cause some rows to be dynamically split into multiple batches during execution and be modified multiple times.

## Failure Recovery
The core component of Non-Transactional DML, the JobController, runs only on the Primary vttablet. When a Primary switch occurs or the Primary vttablet restarts, it ensures consistent job states before and after the event. This is primarily reliant on the following design:
A job consists of three main parts: 1. Metadata stored in memory, used by the JobController to schedule different jobs. 2. State data persisted in database tables. 3. Runner goroutines; when a job is not in the running state, the goroutine exits. Any state generated during the goroutine's operation is written to the database tables as a transaction.
When the Primary vttablet starts, it initializes the JobController, reads all job states from the database tables, and if a job's state is beyond `submitted` but not in a terminal state, it indicates that the vttablet process crashed before the job was completed. For these jobs, corresponding memory metadata is reconstructed. Job scheduling begins only after all metadata recovery is complete.

## Generation of PK Ranges
The Non-Transactional DML tutorial document explained how to define PK ranges for each batch and generate the corresponding batch SQL when the table has a single PK column. When the table has multiple PK columns, the principle remains the same: All rows affected by the DML are sorted by PK, and every batch of rows based on the Batch Size is formed into a batch, with the head and tail values of all PK columns taken as the PK range.
When generating batch SQL, the PK range conditions are formed using `and` and `or`:
For example, if a table has two PK columns of type `int` named `pk1` and `pk2`, and a batch starts with PK values 1 (`pk1`) and 5 (`pk2`) and ends with 2 (`pk1`) and 7 (`pk2`), the condition generated would be:
```sql
(pk1 > 1 or pk1 = 1 and pk2 >= 5) and (pk1 < 2 or pk1 = 2 and pk2 <= 7)
```

## Batch Size
The size of each batch is calculated using the following formula:
```go
batchSize = min(threshod_batch_size, user_input_batch_size)
```
where `user_input_batch_size` is the batch size specified by the user when submitting the job. If the user does not specify it, the default value is `defaultBatchSize`, which can be dynamically configured. `threshold_batch_size` is calculated using the following formula:
```go
default_batch_size = batchSizeThreshold / index_count * ratio
```
where the `batchSizeThreshold` parameter value can be dynamically configured. `index_count` is the number of indexes on the table. `ratio` is a floating-point number between 0 and 1, which can be dynamically configured.

## Content of Show Commands
When a user executes a Show command, the system first queries the job or batch tables. Based on the query results, additional fields can be dynamically added.
For example, the Job table does not contain `affected_rows` and `dealing_batch_id` fields.
`affected_rows`: When querying job table information, the actual number of affected rows for all completed batches associated with the job is aggregated from the batch table and presented as the job's `affected_rows`.
`dealing_batch_id`: When querying job table information, the smallest `id` of the batch with a `queued` status in the batch table associated with the job is presented as the job's `dealing_batch_id`.

## Table GC
The JobController periodically cleans up completed jobs, mainly including: the job entry in the Job table and the batch table associated with the job. For the former, the entry is directly deleted from the Job table. For the latter, the batch table is deleted through a table GC process, specifically by renaming the batch table, which triggers its purge phase.