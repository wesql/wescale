

<div align="center">
    <h1>WeScale</h1>

[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://github.com/wesql/wescale/blob/vitess-release-16.0-dev/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://go.dev/)
<br/>
[![Unit Test (mysql57)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql57.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql57.yml)
[![Unit Test (mysql80)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql80.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql80.yml)
[![E2E Test](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_wesql.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_wesql.yml)
[![MTR Test](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_mysqltester.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_mysqltester.yml)

</div>


WeScale is a database proxy designed to enhance the scalability, performance, security, and resilience of your applications.
<br/>
By managing connections, read-write-split, read-after-write-consistency, load balancing, WeScale offers
a valuable tool for developers and database administrators.

# Getting Started
To [Get Started](doc%2Ftoturial%2F00-Deploy%26Debug.md) with WeScale, simply clone the repository and follow the installation instructions
provided in the documentation.

# Blogs
* [Introduction To WeScale.md](doc%2Fblogs%2FIntroduction%20To%20WeScale.md)
* [Dive into Read-Write-Splitting of WeScale.md](doc%2Fblogs%2FDive%20into%20Read-Write-Splitting%20of%20WeScale.md)
* [Performance Comparison WeScale vs MySQL.md](doc%2Fblogs%2FPerformance%20Comparison%20WeScale%20vs%20MySQL.md)
* [Execution Process of DROP TABLE in OnlineDDL Mode.md](doc%2Fblogs%2FExecution%20Process%20of%20DROP%20TABLE%20in%20OnlineDDL%20Mode.md)

# Documentation
* [Deploy & Debug.md](doc%2Ftoturial%2F00-Deploy%26Debug.md)
* [Architecture.md](doc%2Ftoturial%2F01-Architecture.md)
* [Life of A Query.md](doc%2Ftoturial%2F02-Life%20of%20A%20Query.md)
* [Read-Write-Split & LoadBalancer.md](doc%2Ftoturial%2F03-Read-Write-Split%20%26%20LoadBalancer.md)
* [Read-After-Write-Consistency.md](doc%2Ftoturial%2F04-Read-After-Write-Consistency.md)
* [Transparent Failover.md](doc%2Ftoturial%2F05-Transparent%20Failover.md)
* [Authentication & Authorization.md](doc%2Ftoturial%2F06-Authentication%26Authorization.md)

# Developer
* [Use FailPoint Injection In WeScale.md](doc%2Fdeveloper%2FUse%20FailPoint%20Injection%20In%20WeScale.md)

# Features

**Connection Management:**
WeScale efficiently manages connections to your database, reducing the overhead on your application
and improving performance. WeScale relieves you of the worry of the max_connection problem in your database.

**Read Write Split:**
WeScale simplify application logic by automatically routing read queries to read-only nodes
and write queries to the primary node. This is achieved by parsing and analyzing SQL statements,
which ensures efficient use of available resources.

**Read After Write Consistency:**
When an application writes data to the primary node and subsequently reads it on a read-only node,
WeScale makes sure that the data that was just written to the primary node can be accessed
and read from the read-only node.

**Load Balancing:**
The proxy intelligently routes queries to the appropriate read-only nodes using various load balancing policies.
This ensures that the workload is evenly distributed across all available nodes, optimizing performance
and resource utilization

**Transparent Failover:**
WeScale is capable of automatically detecting failovers and buffering application SQL in its memory while keeping application connections intact, 
thus enhancing application resilience in the event of database failures.

# Contributing
We welcome contributions to WeScale! If you have any ideas, bug reports, or feature requests,
please feel free to open an issue or submit a pull request.

# License
WeScale is released under the Apache 2.0 License.

# Acknowledgements
WeScale is a fork of the Vitess project, which is a database clustering system for horizontal scaling of MySQL.
We would like to thank the Vitess team for their hard work.
