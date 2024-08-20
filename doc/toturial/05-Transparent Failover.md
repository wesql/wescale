---
title: Transparent Failover
---

# Introduction

Failover is a feature designed to ensure that if the original database instance becomes unavailable, it is replaced with another instance and remains highly available. Various factors can trigger a failover event, including issues with the database instance or scheduled maintenance procedures like database upgrades.

Without WeSQL WeScale, a failover requires a short period of downtime. Existing connections to the database are disconnected and need to be reopened by your application. WeSQL WeScale is capable of automatically detecting failovers and buffering application SQL in its memory while keeping application connections intact, thus enhancing application resilience in the event of database failures.

The goal of this tutorial is to explain how to enable the Transparent Failover feature of WeSQL WeScale.

# Setting via launch parameters

```
vtgate \
    # enable transparent failover
    --enable_buffer
    --buffer_size 10000
    --buffer_window 30s
    --buffer_max_failover_duration 60s
    --buffer_min_time_between_failovers 60s
    # other necessary command line options
    ...

```

# Params Explained

| params | descrpition |
| --- | --- |
| enable_buffer | Enables buffering. Not enabled by default |
| buffer_size | Default: 10000 This should be sized to the appropriate number of expected request during a buffering event. Typically, if each connection has one request then, each connection will consume one buffer slot of the buffer_size and be put in a "pause" state as it is buffered on the vtgate. The resource considerations for setting this flag are memory resources. |
| buffer_window | Default: 10s. The maximum time any individual request should be buffered for. Should probably be less than the value for --buffer_max_failover_duration. Adjust according to your application requirements. Be aware, if your MySQL client has write_timeout or read_timeout settings, those values should be greater than the buffer_max_failover_duration. |
| buffer_max_failover_duration | Default: 20s. If buffering is active longer than this set duration, stop buffering and return errors to the client. |
| buffer_min_time_between_failovers | Default 1m. If consecutive fail overs for a shard happens within less than this duration, do not buffer again. The purpose of this setting is to avoid consecutive fail over events where vtgate may be buffering, but never purging the buffer. |

The two most important parameters are `buffer_window` and `buffer_max_failover_duration`.

`buffer_window` controls how long each SQL can be buffered for at most.

`buffer_max_failover_duration` controls how long the "Buffer feature" can last at most.

If either of the two parameters times out, the corresponding SQL will be taken out of the buffer and executed.