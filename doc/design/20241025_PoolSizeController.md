---
title: PoolSizeController
---

# PoolSizeController

- Feature: PoolSizeController
- Start Date: 2024-10-25
- Authors: @earayu

# Dynamic Connection Pool Size Controller

## Overview

The **Dynamic Connection Pool Size Controller** is a feature in **wescale** designed to optimize the management of MySQL connections by dynamically adjusting the sizes of the Transaction Engine (TE) and Query Engine (QE) connection pools. This ensures efficient utilization of resources, prevents exceeding MySQL's `max_connections` limit, and adapts to varying workloads in real-time.

## Motivation

Managing database connections efficiently is crucial for maintaining the performance and stability of any database proxy system. Static configuration of connection pool sizes may lead to underutilization or exhaustion of connections, especially under fluctuating workloads. The Dynamic Connection Pool Size Controller addresses this by:

- **Optimizing Resource Utilization**: Ensuring that available connections are used effectively without exceeding limits.
- **Preventing Connection Errors**: Monitoring MySQL metrics to avoid hitting the `max_connections` limit.
- **Adapting to Workload Changes**: Adjusting pool sizes based on real-time metrics to respond to varying demands.

## Key Features

- **Automatic Adjustment**: Periodically recalculates and applies optimal pool sizes based on current MySQL metrics.
- **Safety Mechanisms**: Incorporates safety buffers and minimum pool sizes to maintain stability.
- **Configurable Parameters**: Offers flexibility through various configuration options to suit different deployment needs.
- **Connection Error Handling**: Monitors `Connection_errors_max_connections` to release idle connections proactively.

## Configuration Parameters

The behavior of the Dynamic Connection Pool Size Controller can be fine-tuned using the following configuration parameters:

- **queryserver_pool_autoscale_enable**:  
  *Type*: Boolean  
  *Description*: Activates the pool size autoscaling feature.  
  *Default*: `false`

- **queryserver_pool_autoscale_dry_run**:  
  *Type*: Boolean  
  *Description*: Enables dry-run mode for testing configurations without applying changes. Overrides `queryserver_pool_autoscale_enable` when set to `true`.  
  *Default*: `false`

- **queryserver_pool_autoscale_interval**:  
  *Type*: Duration  
  *Description*: Sets how frequently the pool sizes are adjusted.  
  *Default*: `30s`

- **queryserver_pool_autoscale_percentage_of_max_connections**:  
  *Type*: Integer (10-90)  
  *Description*: Determines the percentage of MySQL's `max_connections` that wescale can utilize.  
  *Default*: `80`  
  *Range*: `10` to `90`

- **queryserver_pool_autoscale_safety_buffer**:  
  *Type*: Integer  
  *Description*: Reserves a number of connections to prevent exhausting MySQL connections.  
  *Default*: `35`

- **queryserver_pool_autoscale_tx_pool_percentage**:  
  *Type*: Integer (0-100)  
  *Description*: Allocates a fraction of connections to the Transaction Engine (TE) pool.  
  *Default*: `50`  
  *Range*: `0` to `100`

- **queryserver_pool_autoscale_min_tx_pool_size**:  
  *Type*: Integer  
  *Description*: Sets the minimum size for the Transaction Engine (TE) pool.  
  *Default*: `5`

- **queryserver_pool_autoscale_min_oltp_read_pool_size**:  
  *Type*: Integer  
  *Description*: Sets the minimum size for the OLTP Read (QE) pool.  
  *Default*: `5`

## How It Works

1. **Fetching MySQL Metrics**: The controller retrieves `max_connections` and `Connection_errors_max_connections` from MySQL.
2. **Calculating Available Connections**:
    - Computes the maximum number of connections wescale can use (`wescaleMaxConnections`) as a percentage of `max_connections` based on `queryserver_pool_autoscale_percentage_of_max_connections`.
    - Adjusts for the safety buffer to ensure connections are reserved for other operations.
3. **Distributing Connections**:
    - Allocates connections between TE and QE according to `queryserver_pool_autoscale_tx_pool_percentage`.
    - Ensures that both pools meet their minimum size requirements (`queryserver_pool_autoscale_min_tx_pool_size` and `queryserver_pool_autoscale_min_oltp_read_pool_size`).
4. **Applying New Pool Sizes**:
    - Updates the pool sizes in the TE and QE.
    - Logs the adjustments for transparency and monitoring.
5. **Handling Connection Errors**:
    - Monitors `Connection_errors_max_connections` for any increases.
    - If an increase is detected, it releases idle connections to alleviate pressure on MySQL.

## Usage

To enable and configure the Dynamic Connection Pool Size Controller in **wescale**, modify the configuration file with the appropriate key-value pairs.

### Configuration File Example

```cnf
queryserver_pool_autoscale_enable=true
queryserver_pool_autoscale_dry_run=false
queryserver_pool_autoscale_interval=30s
queryserver_pool_autoscale_percentage_of_max_connections=75
queryserver_pool_autoscale_safety_buffer=30
queryserver_pool_autoscale_tx_pool_percentage=60
queryserver_pool_autoscale_min_tx_pool_size=10
queryserver_pool_autoscale_min_oltp_read_pool_size=10
```

### Steps to Enable

1. **Enable the Feature**:  
   Set `queryserver_pool_autoscale_enable` to `true` to activate the autoscaling feature.

2. **Configure Parameters**:  
   Adjust the parameters such as `queryserver_pool_autoscale_percentage_of_max_connections`, `queryserver_pool_autoscale_safety_buffer`, `queryserver_pool_autoscale_tx_pool_percentage`, etc., to match your deployment needs.

3. **Dry-Run Mode (Optional)**:  
   To test configurations without affecting the actual pool sizes, set `queryserver_pool_autoscale_dry_run` to `true`. This allows you to verify the behavior before applying changes.

4. **Monitor Logs**:  
   Check the wescale logs to observe how the controller adjusts the pool sizes and handles connection errors. This helps in ensuring that the controller operates as expected.

## Best Practices

- **Start with Dry-Run**:  
  Use the dry-run mode to ensure that the controller behaves as expected before enabling it in a production environment.

- **Monitor MySQL Metrics**:  
  Keep an eye on MySQL's `max_connections` and related metrics to adjust the controller's parameters appropriately.

- **Adjust Safety Buffer**:  
  Ensure that the `queryserver_pool_autoscale_safety_buffer` is sufficient to prevent connection exhaustion due to other processes or sudden spikes in demand.

- **Customize for Workloads**:  
  Tune `queryserver_pool_autoscale_tx_pool_percentage` and minimum pool sizes based on the read/write characteristics of your workload.

## Limitations

- **Role Awareness**:  
  Currently, the controller does not adjust pool sizes differently for read-only or leader nodes. Future enhancements may include role-based adjustments.

- **Adaptive Allocation**:  
  The controller does not yet adapt pool sizes based on the actual usage patterns of TE and QE. This could be improved in future versions.

- **MySQL Overhead**:  
  Frequent queries to MySQL for metrics may introduce slight overhead. Adjust the `queryserver_pool_autoscale_interval` parameter to balance responsiveness and performance.

## Future Enhancements

- **Workload-Based Adjustments**:  
  Incorporate logic to adjust pool sizes based on real-time usage statistics of the TE and QE.

- **Node Role Differentiation**:  
  Implement role-based pool size adjustments for leader and follower nodes.

- **Dynamic Configuration Reload**:  
  Allow for configuration changes without restarting the wescale service.

- **Enhanced Monitoring**:  
  Integrate with monitoring tools to visualize pool size changes and MySQL metrics over time.

## Conclusion

The **Dynamic Connection Pool Size Controller** enhances wescale's ability to manage database connections efficiently. By automatically adjusting pool sizes in response to real-time metrics, it helps maintain optimal performance and stability, ensuring that applications using wescale can handle varying workloads gracefully.
