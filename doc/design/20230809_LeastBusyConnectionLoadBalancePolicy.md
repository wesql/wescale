---
title: Dive into Read-Write-Splitting of WeScale
---

- Feature Name:
- Start Date: 2023-08-09
- Authors: lyq10085
- Issue: https://github.com/apecloud/WeSQL WeScale/issues/220
- PR: https://github.com/apecloud/WeSQL WeScale/pull/230

# Summary

The proposal aims to add new policy (LEAST_MYSQL_CONNECTED_CONNECTIONS, LEAST_MYSQL_RUNNING_CONNECTIONS, LEAST_TABLET_INUSE_CONNECTIONS) to vtgate's existing load balancer. The new policy will enable vtgate to pick vttablet with the least busy connections to its managed mysqld instance or vttablet which manages the mysqld with the minimal busy connections.

# Motivation

In practice, the number of connections in mysqld reflects the load on vttablet and mysqld processes. The load balancing strategy of routing requests based on mysql connection count will improve qps of the WeSQL WeScale cluster.

# Technical design

## Goals

1. LEAST_MYSQL_CONNECTED_CONNECTIONS: vtgate routes queries based on Mysql connected connections.
2. LEAST_MYSQL_RUNNING_CONNECTIONS: vtgate routes queries based on Mysql Running connections.
3. LEAST_TABLET_INUSE_CONNECTIONS: vtgate routes queries based on vttablet connection pool usage status.

## Design Details

1. vttablet' tabletserver use healthstreamer to collect various connection pools' usage status as well as underlying mysqld's connection status periodically, wrapped such status as subfields of a streamhealthreponse ready to stream through grpc. 
2. vtgate' tabletgateway streams vttablets' connection pool usage status through streamHealth grpc and cached it into tabletgateway's healtcheck data.
3. vtgate' query executor picks up tablet based on vtgate's tabletgateway cached healthcheck data.

## Road Map

- [x] implement ThreadsStatus method of statemanager's replTracker.

- [x] impelement queryEngine and TxEngine getConnPoolInUseStats method 

- [x] add update heathstreamer's RealTimeStats logic to statemanager's broadcast method

- [x] implement leastMysqlConnectedConnections, leastMysqlRunningConnections, leastTabletInUseConnections method of tabletgateway' load balancer.

# Usage

```MySQL
set @@read_write_splitting_policy='least_mysql_connected_connections';
set @@read_write_splitting_policy='least_mysql_running_connections';
set @@read_write_splitting_policy='least_tablet_inuse_connections';
```

# Future Works

Enable vtgate to actively check vttablet connection pools' usage status.

# References

[WeSQL WeScale load balancer](https://github.com/apecloud/WeSQL WeScale/issues/64)