

<div align="center">
    <h1>WeSQL-Scale</h1>

[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://github.com/apecloud/wesql-scale/blob/vitess-release-16.0-dev/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://go.dev/)
<br/>
[![unit test](https://github.com/apecloud/wesql-scale/actions/workflows/unit_test_mysql80.yml/badge.svg)](https://github.com/apecloud/wesql-scale/actions/workflows/unit_test_mysql80.yml)
[![unit test](https://github.com/apecloud/wesql-scale/actions/workflows/unit_test_mysql57.yml/badge.svg)](https://github.com/apecloud/wesql-scale/actions/workflows/unit_test_mysql57.yml)
[![e2e test](https://github.com/apecloud/wesql-scale/actions/workflows/cluster_endtoend_wesql.yml/badge.svg)](https://github.com/apecloud/wesql-scale/actions/workflows/cluster_endtoend_wesql.yml)
[![MTR test](https://github.com/apecloud/wesql-scale/actions/workflows/cluster_endtoend_mysqltester.yml/badge.svg)](https://github.com/apecloud/wesql-scale/actions/workflows/cluster_endtoend_mysqltester.yml)

</div>


WeSQL-Scale is a database proxy designed to enhance the scalability, performance, security, and resilience of your applications.
<br/>
By managing connections, read-write-split, read-after-write-consistency, load balancing, WeSQL-Scale offers
a valuable tool for developers and database administrators.

# Getting Started
To get started with WeSQL-Scale, simply clone the repository and follow the [installation instructions](https://github.com/apecloud/wesql-scale/blob/main/doc/dev_docs/00-Deploy%26Debug.md)
provided in the documentation.

# Features

**Connection Management:**
WeSQL-Scale efficiently manages connections to your database, reducing the overhead on your application
and improving performance. WeSQL-Scale relieves you of the worry of the max_connection problem in your database.

**Read Write Split:**
WeSQL-Scale simplify application logic by automatically routing read queries to read-only nodes
and write queries to the primary node. This is achieved by parsing and analyzing SQL statements,
which ensures efficient use of available resources.

**Read After Write Consistency:**
When an application writes data to the primary node and subsequently reads it on a read-only node,
WeSQL-Scale makes sure that the data that was just written to the primary node can be accessed
and read from the read-only node.

**Load Balancing:**
The proxy intelligently routes queries to the appropriate read-only nodes using various load balancing policies.
This ensures that the workload is evenly distributed across all available nodes, optimizing performance
and resource utilization

**Transparent Failover:**
WeSQL-Scale is capable of automatically detecting failovers and buffering application SQL in its memory while keeping application connections intact, 
thus enhancing application resilience in the event of database failures.

# Contributing
We welcome contributions to WeSQL-Scale! If you have any ideas, bug reports, or feature requests,
please feel free to open an issue or submit a pull request.

# License
WeSQL-Scale is released under the Apache 2.0 License.

# Acknowledgements
WeSQL-Scale is a fork of the Vitess project, which is a database clustering system for horizontal scaling of MySQL.
We would like to thank the Vitess team for their hard work.
