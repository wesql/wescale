---
title: The Lifecycle of Alter Table Statements
---

# **In-Depth Understanding of Vitess Online DDL: The Lifecycle of Alter Table Statements**

**Lifecycle of Alter Table in Online DDL**

1. How DDL SQL is parsed.

   After the DDL SQL is sent to Vtgate, it goes through a parser to generate a plan. There are generally two types of plans: normalDDL and onlineDDL. normalDDL is directly executed on the MySQL instance.

2. How the onlineDDL plan is created.

   When **`ddl_strategy='vitess'`** or **`ddl_strategy='online'`** is set, vtgate parses the SQL to extract the necessary information for onlineDDL and generates a unique uuid for that particular onlineDDL. The original statement **`alter table t1 add column v3 int;`** is rewritten as **`alter /*vt+ uuid="..." context="..." tableSchema="..." table="..." strategy="..." options="" */ table test.t1 add column v3 int`**. This format helps vttablet to parse and obtain related information after receiving the SQL, which is then sent to the Primary Tablet.

3. The process of finally inserting the DDL plan into the **`schema_migrations`** table.

   At the vttablet side, after parsing the SQL into an onlineDDL object and performing DDL conflict verification, it is inserted into the **`mysql.schema_migrations`** table. The entries in the **`mysql.schema_migrations`** table can be considered as onlineDDL tasks, with an initial state of **`queued`**.

   The states of onlineDDL include **`queued`**, **`ready`**, **`running`**, **`complete`**, **`fail`**, **`cancel`**, **`pause`**:

    - **`queued`**: Newly created onlineDDL, the initial state.
    - **`ready`**: Online DDL prepared for execution.
    - **`running`**: Online DDL in progress, including the creation of shadow tables, generation of select and insert statements, and initiation of Vreplication tasks.
    - **`complete`**: Online DDL execution finished, entering this state after cutover completion.
    - **`fail`**: Execution fails due to internal errors. Error details can be diagnosed using the **`message`** field with the corresponding uuid.
    - **`cancel`**: WeSQL provides **`alter schema_migration`** to cancel an online DDL task. Tasks in this state can be re-executed using the **`retry`** command.
    - **`pause`**: Currently under development, this feature can pause online DDL tasks in **`queue`**, **`ready`**, or **`running`** states.

   For state transitions, refer to the article [OnlineDDLScheduler](20231113_OnlineDDLScheduler.md)

   For alter statements, onlineDDL mainly performs three actions post-task issuance:

    1. **Creating a Shadow Table (Scheduler)**
        - This task is executed by the scheduler located in the primary tablet.
        - It involves setting up a shadow table that will eventually replace the original table.
    2. **Copying Data (Vreplication)**
        - Handled by a separate component known as Vreplication.
        - This component is responsible for both full and incremental copying of data from the original table to the shadow table, refer to the article [Vreplication](20231113_Vreplication.md).
    3. **Switching Between Source and Shadow Tables (Cutover)**
        - This step occurs when the difference between the shadow table and the source table falls below a specified threshold.
        - At this point, the source table is locked to prevent Data Manipulation Language (DML) operations, ensuring data consistency.
        - The system waits until the shadow table catches up with the source table.
        - Once synchronized, a cutover is performed where the shadow table becomes the new source table, completing the alter process.