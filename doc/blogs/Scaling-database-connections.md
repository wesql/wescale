---
title: Scaling Database Connections
date: '2024-11-15'
tags: ['database', 'proxy', 'connection pool']
draft: false
summary: This article explains the challenges of scaling database connections and how connection pooling and multiplexing can address them.
---

> [WeScale](https://github.com/wesql/wescale): A Database Proxy

Have you ever wonder why do databases limit the maximum number of connections?

Each connection consumes valuable resources like memory and file descriptors, potentially affecting performance. As systems grow and demand more connections, these limits hinder scalability. Connection pooling offers a solution.

A database connection pool addresses two critical issues in high-performance systems:
* **Connection Creation Overhead**: Establishing new database connections is expensive due to TCP handshakes, authentication, encryption, and setup tasks. Frequent connection creation adds latency, becoming a bottleneck under high load. **Pooling connections** mitigates this issue by reusing established connections, reducing overhead.
* **Limited Database Connections**: Each connection consumes memory and server-side resources. As applications scale, the number of connections can quickly exceed what the database can efficiently handle. **Connection multiplexing** allows multiple clients to share fewer physical connections, alleviating the database server's load.

When discussing how to implement a Connection Pool, some find it straightforward, while others see it as challenging. Both perspectives are valid. **Pooling is simple**—it involves maintaining an efficient data structure to store and retrieve connections. **Multiplexing is hard** because it requires efficient management of connection states.

## Client-Side Connection Pool vs. Server-Side Connection Pool

Choosing between a **client-side connection pool** and a **server-side connection pool** is critical. For large systems, server-side pooling is more scalable and controlled.

**Why Client-Side Connection Pools Often Fail at Scale?**

Client-side connection pools may suffice for smaller systems but become liabilities as client numbers increase. Since clients are typically stateless, scaling out leads to an exponential rise in connections, resulting in:

- Excessive Resource Usage: Each client manages its own pool, consuming more connections than necessary.
- Uneven Load Distribution: Persistent, long-lived client connections create load imbalances, with some database servers overwhelmed while others remain idle.

The clear conclusion: Server-side connection pooling is essential for scalability. Centralizing connection management on the server provides better control, efficient connection sharing, and optimal performance.

## Connection Multiplexing: The Heart of Scalability

The core of **connection multiplexing** is effective state management. A single connection should serve multiple clients, but only if their states are properly isolated. Otherwise, the state left by one client could affect another, leading to data corruption or security breaches.

Connection state involves:

- User variables
- Session variables
- Transaction status
- Temporary tables
- **Current connected user**
- **Current used database**

Efficiently managing these states is key to enabling connection multiplexing at scale, directly addressing the challenge of limited connections.

### Multiplexing Granularity: Balancing Efficiency and Isolation

The **granularity of multiplexing** determines how connections are shared. Finer granularity increases reuse and reduces the number of needed connections but complicates state management.

The multiplexing granularity can be categorized into three types:
- **Session Pooling**: A connection is checked out when a client session begins and returned when the session ends. The connection remains dedicated to that client for the entire session, maintaining all session states.
- **Transaction Pooling**: A connection is checked out at the start of a transaction and returned immediately after the transaction commits or rolls back. This ensures transaction integrity while allowing connections to be reused between transactions.
- **Statement Pooling**: A connection is checked out for the execution of a single SQL statement and returned immediately after. This maximizes connection reuse but requires resetting the connection state between statements.

WeScale balances efficiency and isolation by adapting the pooling strategy based on the query type—using **Statement Pooling** for stateless, read-only queries and **Transaction Pooling** for write-heavy or transactional queries.

### How WeScale Handles Connection Multiplexing

In WeScale, when a client checks out a connection, any changes to the connection’s state (like session variables or transaction states) are recorded. Upon returning the connection to the pool, WeScale resets the state, ensuring a clean state for the next client. All the connections in the pool are identical, allowing them to be shared across multiple clients freely.

However, certain states—such as the **current used database** and **current connected user**—are more challenging to manage and are almost always modified during checkout. These states are crucial because they are specified by the application at connection time.

- **Current Used Database**: Clients typically specify a database context before executing queries. Switching databases within a connection requires sending a command to the database (e.g., `USE <database>`), introducing additional **round-trip latency**.

- **Current Connected User**: The connected user also needs to be specified by client at connection time. Changing the user for a connection from the pool involves authentication checks and permissions, and frequent changes can degrade performance.

### Optimizing Connection's Database State: DatabaseName Rewrite

A key optimization in WeScale's design is the **DatabaseName rewrite**. This technique parses SQL queries to prepend the current used database name to table names within the query itself. This allows the **same connection** to execute queries across multiple databases without frequent state resets or context switches.

**Example:**

- **Input Query:**

```sql
SELECT (SELECT d FROM t2 WHERE d > a), t1.* FROM t1;
  ```

- **Transformed Query:**

```sql
-- Suppose `test` is the current used database
SELECT (SELECT d FROM test.t2 WHERE d > a), t1.* FROM test.t1;
  ```

By transforming the query to include the database name explicitly, WeScale avoids relying on the connection's current used database. This reduces overhead and enhances the flexibility and reusability of connections across different client sessions.

### Optimizing Connection's User State: Authentication and Authorization in Proxy

One challenge of connection pooling is dealing with varying user credentials and permissions. Changing users on an active connection is resource-intensive and requires state resets. WeScale simplifies this by maintaining a uniform user across all pooled connections. This allows the connection’s user state to remain constant, even as different client requests arrive, reducing unnecessary state changes and optimizing performance.

**But how about user authentication and authorization?**

WeScale ensures MySQL compatibility by supporting standard authentication methods like `mysql_native_password` and `caching_sha2_password`. The proxy handles user authentication and authorization directly against MySQL’s internal user database, eliminating the need for separate authentication systems or custom credentials management.

Unlike other proxies that require users to configure additional credentials or rely on external systems, WeScale leverages MySQL’s native authentication system, ensuring better security and compatibility.

## Summary

Scaling database connections is challenging due to resource limits and connection overhead. Client-side connection pools often fail at scale, leading to excessive resource usage and uneven load. WeScale tackles this by implementing server-side connection pooling and efficient connection multiplexing, allowing multiple clients to share fewer connections without state conflicts. Key strategies include:

- **Adaptive Pooling**: Using statement pooling for read-only queries and transaction pooling for transactional ones.
- **State Management**: Resetting connection states upon return to the pool to ensure isolation, making sure that connections are clean for the next client.
- **DatabaseName Rewrite**: Modifying queries to include explicit database names, reducing the need for resetting the connection’s current used database.
- **Proxy Authentication**: Handling user authentication within the proxy, reducing the need for changing the connection's current connected user.

These approaches reduce overhead, optimize resource usage, and enhance scalability and performance in high-demand database environments.
