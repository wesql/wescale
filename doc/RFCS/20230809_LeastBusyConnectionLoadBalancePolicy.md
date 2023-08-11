- Feature Name:
- Start Date: 2023-08-09
- Authors: lyq10085
- Issue: https://github.com/apecloud/wesql-scale/issues/220
- PR: https://github.com/apecloud/wesql-scale/pull/230

# Summary

The proposal aims to add new policy (LEAST_BUSY_CONNECTIONS, LEAST_GLOBAL_BUSY_CONNECTIONS) to vtgate's existing load balancer. The new policy will enable vtgate to pick vttablet with the least busy connections to its managed mysqld instance or vttablet which manages the mysqld with the minimal busy connections.

# Motivation

In practice, the number of connections in mysqld reflects the load on vttablet and mysqld processes. The load balancing strategy of routing requests based on mysql connection count will improve qps of the wesql-scale cluster.

# Technical design

## Goals

1. LEAST_BUSY_CONNECTIONS: vtgate routes queries based on cached vttablet connection pools' usage status.
2. LEAST_GLOBAL_BUSY_CONNECTIONS: vtgate routes queries based on underlying mysqld connection status.

## Design Details

1. vttablet' tabletserver use healthstreamer to collect various connection pools' usage status as well as underlying mysqld's connection status periodically, wrapped such status as subfields of a streamhealthreponse ready to stream through grpc. 
2. vtgate' tabletgateway streams vttablets' connection pool usage status through streamHealth grpc and cached it into tabletgateway's healtcheck data.
3. vtgate' query executor picks up tablet based on vtgate's tabletgateway cached healthcheck data.

## Road Map

- [x] implement checkMysqlConnections and checkTabletPoolUsage method of tabletserver's healthstreamer.

- [x] implement leastbusyconnections and leastglobalbusyconnections method of tabletgateway' load balancer.

# Usage

```MySQL
set session read_write_splitting_policy='least_busy_connections';
set session read_write_splitting_policy='least_global_busy_connections';
```

# Future Works

Enable vtgate to actively check vttablet connection pools' usage status.

# References

[wesql-scale load balancer](https://github.com/apecloud/wesql-scale/issues/64)