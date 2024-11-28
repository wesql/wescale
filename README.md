

<div align="center">
    <h1>WeScale</h1>

[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://github.com/wesql/wescale/blob/vitess-release-16.0-dev/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://go.dev/)
<br/>
[![Unit Test (mysql57)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql57.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql57.yml)
[![Unit Test (mysql80)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql80.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/unit_test_mysql80.yml)
[![E2E Test](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_wesql.yml/badge.svg?branch=main)](https://github.com/wesql/wescale/actions/workflows/cluster_endtoend_wesql.yml)
[![MTR Test](https://github.com/wesql/wescale/actions/workflows/wescale_wesql_performance_docker.yml/badge.svg)](https://github.com/wesql/wescale/actions/workflows/wescale_wesql_performance_docker.yml)

</div>


# Introduction
WeScale is an open-source database proxy built with application developers in mind. It aims to simplify and enhance the 
interaction between your application and the database, making development more efficient and enjoyable.
<br/>
While databases have evolved to include complex and common application requirements like transactions and indexing, 
modern applications have a growing set of shared needs that aren’t suitable or haven’t yet been integrated into the database layer. 
This gap has led to the rise of various database middleware solutions such as 
client-side connection pools, SQL retry libraries, online DDL tools, and more.
<br/>
As a database proxy, WeScale serves as a crucial bridge between the application layer and the database layer.
It leans towards the application side, offering features specifically designed to simplify the developer experience. These include:
- **Server Side Connection Management**: Efficiently manages database connections to prevent issues like max_connections limits.
- **Read-Write Splitting**: Automatically routes read queries to replicas and write queries to the primary node.
- **Read-After-Write Consistency**: Ensures that data written to the primary node is immediately available on replicas for subsequent reads.
- **Load Balancing**: Distributes queries intelligently across replicas to optimize performance.
- **Transparent Failover**: Automatically handles database failovers while keeping application connections intact.
- DeclartiveDDL & OnlineDDL: Simplifies schema changes by allowing developers to define schema changes declaratively and apply them online.
- Branch: Enables developers to create a branch of the database for testing and development purposes.
- Transaction Chopping: Splits large transactions into smaller chunks to prevent long-running transactions from blocking other queries.
- Filters & Wasm Plugin: Allows developers to write custom filters using WebAssembly to extend WeScale's functionality.
- Native Authentication & Authorization: Supports MySQL native authentication and authorization methods.

# Getting Started
* To [Get Started On Your Local Machine](doc%2Ftoturial%2F00-Deploy%26Debug.md) with WeScale, simply clone the repository and follow the installation instructions
provided in the documentation.
* To [Deploy WeScale on Kubernetes](doc%2Ftoturial%2F11-Getting-Started-with-Kubernetes.md), 
you can use the powerful Kubeblocks Operator to quickly launch a WeScale cluster in a Kubernetes cluster. We recommend this method for production environments. 

## Getting Started with Docker
To Start WeScale with Docker, you can simply run the following command:
```shell
docker network create wescale-network

docker run -itd --network wescale-network --name mysql-server \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_ROOT_HOST=% \
  -e MYSQL_LOG_CONSOLE=true \
  mysql/mysql-server:8.0.32 \
  --bind-address=0.0.0.0 \
  --port=3306 \
  --log-bin=binlog \
  --gtid_mode=ON \
  --enforce_gtid_consistency=ON \
  --log_replica_updates=ON \
  --binlog_format=ROW

docker run -itd --network wescale-network --name wescale \
  -p 15306:15306 \
  -w /vt/examples/wesql-server \
  -e MYSQL_ROOT_USER=root \
  -e MYSQL_ROOT_PASSWORD=passwd \
  -e MYSQL_PORT=3306 \
  -e MYSQL_HOST=mysql-server \
  apecloud/apecloud-mysql-scale:0.3.7 \
  /vt/examples/wesql-server/init_single_node_cluster.sh
```

Now you can connect to WeScale and the original MySQL server:
```shell
# Connect to WeScale
mysql -h127.0.0.1 -uroot -ppasswd -P15306

# You can still connect to the original MySQL server
mysql -h127.0.0.1 -uroot -ppasswd -P3306
```

Clean up the containers:
```shell
docker rm -f mysql-server wescale
docker network rm wescale-network
```

# Documentation
* [Read-Write-Split & LoadBalancer.md](doc%2Ftoturial%2F03-Read-Write-Split%20%26%20LoadBalancer.md)
* [Read-After-Write-Consistency.md](doc%2Ftoturial%2F04-Read-After-Write-Consistency.md)
* [Declarative-DDL.md](doc%2Ftoturial%2F14-Declarative-DDL.md)
* [OnlineDDL.md](doc%2Ftoturial%2F07-OnlineDDL.md)
* [Branch](doc%2Ftoturial%2F08-Branch.md)
* [Transaction Chopping.md](doc%2Ftoturial%2F09-Transaction-Chopping.md)
* [Show Tablets Query Plans](doc%2Ftoturial%2F10-Show%20Tablets%20Query%20Plans.md)
* [Filters](doc%2Ftoturial%2F12-Filters.md)
* [Write a Wasm Plugin In WeScale](doc%2Ftoturial%2F13-Write-a-Wasm-Plugin-In-WeScale.md)
* [Transparent Failover.md](doc%2Ftoturial%2F05-Transparent%20Failover.md)
* [Authentication & Authorization.md](doc%2Ftoturial%2F06-Authentication%26Authorization.md)

# Blogs
* [Architecture.md](doc%2Ftoturial%2F01-Architecture.md)
* [Life of A Query.md](doc%2Ftoturial%2F02-Life%20of%20A%20Query.md)
* [Introduction To WeScale.md](doc%2Fblogs%2FIntroduction%20To%20WeScale.md)
* [Dive into Read-Write-Splitting of WeScale.md](doc%2Fblogs%2FDive%20into%20Read-Write-Splitting%20of%20WeScale.md)
* [Performance Comparison WeScale vs MySQL.md](doc%2Fblogs%2FPerformance%20Comparison%20WeScale%20vs%20MySQL.md)
* [Execution Process of DROP TABLE in OnlineDDL Mode.md](doc%2Fblogs%2FExecution%20Process%20of%20DROP%20TABLE%20in%20OnlineDDL%20Mode.md)
* [Scaling-database-connections.md](doc%2Fblogs%2FScaling-database-connections.md)

# Developer
* [Use FailPoint Injection In WeScale.md](doc%2Fdeveloper%2FUse%20FailPoint%20Injection%20In%20WeScale.md)

# Contributing
We welcome contributions to WeScale! If you have any ideas, bug reports, or feature requests,
please feel free to open an issue or submit a pull request.

# License
WeScale is released under the Apache 2.0 License.

# Acknowledgements
WeScale is a fork of the Vitess project, which is a database clustering system for horizontal scaling of MySQL.
We would like to thank the Vitess team for their hard work.
