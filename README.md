[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://github.com/apecloud/wesql-scale/blob/vitess-release-16.0-dev/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://go.dev/)

# WeSQL-Scale 

wesql-scale is designed to enhance the scalability, performance, security, and resilience of your applications. 
By managing connections, read-write-split, read-after-write-consistency, load balancing, wesql-scale offers 
a valuable tool for developers and database administrators.

# Features

## Connection Management
wesql-scale efficiently manages connections to your database, reducing the overhead on your application 
and improving performance. wesql-scale relieves you of the worry of the max_connection problem in your database.

## Read Write Split
wesql-scale simplify application logic by automatically routing read queries to read-only nodes 
and write queries to the primary node. This is achieved by parsing and analyzing SQL statements, 
which ensures efficient use of available resources.

## Read After Write Consistency
When an application writes data to the primary node and subsequently reads it on a read-only node, 
wesql-scale makes sure that the data that was just written to the primary node can be accessed 
and read from the read-only node.

## Load Balancing
The proxy intelligently routes queries to the appropriate read-only nodes using various load balancing policies. 
This ensures that the workload is evenly distributed across all available nodes, optimizing performance 
and resource utilization

# Getting Started
To get started with wesql-scale, simply clone the repository and follow the [installation instructions](https://github.com/apecloud/wesql-scale/blob/vitess-release-16.0-dev/doc/dev_docs/00-Deploy%26Debug.md) 
provided in the documentation.

# Contributing
We welcome contributions to wesql-scale! If you have any ideas, bug reports, or feature requests, 
please feel free to open an issue or submit a pull request.

# License
wesql-scale is released under the Apache 2.0 License.

# Acknowledgements
wesql-scale is a fork of the Vitess project, which is a database clustering system for horizontal scaling of MySQL. 
We would like to thank the Vitess team for their hard work.
