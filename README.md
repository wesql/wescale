

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
It leans towards the application side, offering features specifically designed to simplify the developer experience. 

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
  apecloud/apecloud-mysql-scale:0.3.8 \
  /vt/examples/wesql-server/init_single_node_cluster.sh
```

Now you can connect to WeScale and the original MySQL server:
```shell
# Connect to WeScale
mysql -h127.0.0.1 -uroot -ppasswd -P15306

# You can still connect to the original MySQL server
mysql -h127.0.0.1 -uroot -ppasswd -P3306
```

Try out Declarative DDL:
```sql
CREATE DATABASE if not exists test;
USE test;

CREATE TABLE if not exists test_table (
    id int primary key,
    name varchar(255)
);

SHOW CREATE TABLE test_table;

CREATE TABLE if not exists test_table (
    id int primary key,
    name varchar(255),
    email varchar(255),
    profile varchar(255),
    index (name, email)
);

SHOW CREATE TABLE test_table;
```

Clean up the containers:
```shell
docker rm -f mysql-server wescale
docker network rm wescale-network
```

## Community
Join our [Discord](https://discord.com/channels/1308609231498510427/1308609231498510430) to discuss features, get help, and connect with other users.

# Features
* [Declarative-DDL.md](doc%2Ftoturial%2F14-Declarative-DDL.md)
* [OnlineDDL.md](doc%2Ftoturial%2F07-OnlineDDL.md)
* [Transaction Chopping.md](doc%2Ftoturial%2F09-Transaction-Chopping.md)
* [Filters](doc%2Ftoturial%2F12-Filters.md)
* [Write a Wasm Plugin In WeScale](doc%2Ftoturial%2F13-Write-a-Wasm-Plugin-In-WeScale.md)
* [Branch](doc%2Ftoturial%2F08-Branch.md)
* [Show Tablets Query Plans](doc%2Ftoturial%2F10-Show%20Tablets%20Query%20Plans.md)
* [Transparent Failover.md](doc%2Ftoturial%2F05-Transparent%20Failover.md)
* [Authentication & Authorization.md](doc%2Ftoturial%2F06-Authentication%26Authorization.md)
* [Read-Write-Split & LoadBalancer.md](doc%2Ftoturial%2F03-Read-Write-Split%20%26%20LoadBalancer.md)
* [Read-After-Write-Consistency.md](doc%2Ftoturial%2F04-Read-After-Write-Consistency.md)

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

# Monitoring
Once you have WeScale up and running, you can monitor the cluster using prometheus and grafana.
```bash
cd ./examples/metrics && ./start_dashboard.sh
```
Open your browser and navigate to `http://localhost:3000/dashboards` to view the dashboard.

![20241217-164819.jpeg](doc%2F20241217-164819.jpeg)

# Contributing
We welcome contributions to WeScale! If you have any ideas, bug reports, or feature requests,
please feel free to open an issue or submit a pull request.

# License
WeScale is released under the Apache 2.0 License.

# Acknowledgements
WeScale is a fork of the Vitess project, which is a database clustering system for horizontal scaling of MySQL.
We would like to thank the Vitess team for their hard work.
