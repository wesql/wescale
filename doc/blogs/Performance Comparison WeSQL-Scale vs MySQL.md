# Performance Comparison: WeSQL-Scale vs MySQL

# Introduction

WeSQL-Scale is a database proxy that is compatible with MySQL. It supports read-write splitting without stale reads, connection pooling, and transparent failover. These features work together to improve application scalability, performance, security, and resilience. In this article, we will compare the performance of WeSQL-Scale and MySQL, and examine how these features affect throughput and latency. Let's get started!

For those in a hurry, here are the main findings:

- Adding WeSQL-Scale as a proxy for MySQL may cause some additional network round trips, but despite this, its overall throughput and latency under high load conditions are similar to a direct connection.
- When the read-write-split feature is activated, WeSQL-Scale demonstrates linear throughput growth even after MySQL reaches its bottleneck, as the number of pressure testing threads gradually increases.
- The resource overhead introduced by WeSQL-Scale accounts for only about 1/3 of the total resource consumption. The cost-effectiveness of increasing QPS through read-write-splitting and horizontal scaling with WeSQL-Scale is higher compared to the cost-effectiveness of vertical scaling with MySQL.

# Benchmarking WeSQL-Scale and MySQL

## Environment

> kubernetes version：1.26.3
> 
> 
> Load balancer: slb.s2.small
> 
> Machine: 4C16G
> 
> SysBench: balanced read write workload
> 

We have deployed a WeSQL-Scale cluster in Kubernetes. SysBench can connect to both VTGate and MySQL directly, allowing us to compare their performance. The MySQL cluster consists of one Leader and two Followers. VTTablet is deployed as a sidecar in the MySQL Pod. VTGate and SysBench are deployed in their respective Pods. Each Pod will have exclusive access to a 4-core 16GB cloud server.

![](images/16923404829726.jpg)


## MySQL vs WeSQL-Scale (With Read Write Split Disabled)


| Threads | WeSQL-Scale QPS | WeSQL-Scale Latency | MySQL QPS | MySQL Latency |
| --- | --- | --- | --- | --- |
| 4.00 | 2858.98 | 139.77 | 4638.01 | 86.20 |
| 8.00 | 4386.4 | 182.21 | 7508.50 | 106.48 |
| 16 | 6698.62 | 238.58 | 9654.44 | 165.67 |
| 25 | 8596.97 | 290.47 | 10968.65 | 227.77 |
| 50 | 11576.21 | 431.36 | 12440.1 | 401.39 |
| 75 | 12242.44 | 611.36 | 13055.7 | 573.76 |
| 100 | 12488.25 | 798.48 | 12998.67 | 767.69 |
| 125 | 12551.4 | 1389.39 | 12772.8 | 1366.46 |
| 150 | 12630.16 | 1579.17 | 12351.89 | 1614.56 |
| 175 | 12843 | 1745.59 | 12024.3 | 1864.79 |
| 200 | 12612.59 | 1972.6 | 11686.86 | 2132.42 |


![read_write_split_disable_qps_latency.png](images%2Fread_write_split_disable_qps_latency.png)


Some Observations:

- When the sysbench thread count is low, WeSQL-Scale's throughput is about one-third lower than when directly connecting to MySQL, and its latency is also higher. However, as the thread count increases, the performance and throughput of WeSQL-Scale begin to approach those of a direct MySQL connection.
- By utilizing a connection pool and reusing its connections, the performance overhead brought about by incorporating WeSQL-Scale is effectively concealed.

During the testing process, we found that MySQL couldn't handle too many sysbench threads due to the limitation of max_connection. We had to modify the max_connection parameter in order to test with 175 sysbench threads. However, with max_connections set to 100 in MySQL, WeSQL-Scale can handle over 5000 sysbench threads (we are unable to increase the number of sysbench threads further due to memory constraints on the server).

The above test proves a point: introducing WeSQL-Scale as a proxy can increase the scalability and resilience of the database. It will not compromise much of the performance, including QPS and Latency.

## MySQL vs WeSQL-Scale ( With Read Write Split Enable)

| Threads | WeSQL-Scale QPS | WeSQL-Scale Latency | MySQL QPS | MySQL Latency |
| --- | --- | --- | --- | --- |
| 4 | 2858.98 | 139.77 | 4638.01 | 86.2 |
| 8 | 4202.61 | 190.12 | 5252.91 | 152.18 |
| 16 | 6983.17 | 228.97 | 8303.15 | 192.53 |
| 25 | 9583.73 | 260.51 | 12807.06 | 195.02 |
| 50 | 12689.05 | 393.76 | 12631.39 | 395.28 |
| 75 | 15703.01 | 476.59 | 12642.1 | 592.75 |
| 100 | 17772.62 | 560.7 | 12907.98 | 773.19 |
| 125 | 19049.49 | 655.37 | 13028 | 957.99 |
| 150 | 20509.19 | 729.86 | 12994.45 | 1152.22 |
| 175 | 21289.84 | 820.12 | 12910.42 | 1352.38 |
| 200 | 22398.34 | 931.08 | 12537.16 | 1531.23 |

![read_write_split_enable_qps_latency.png](images%2Fread_write_split_enable_qps_latency.png)


Some Observations:

- When WeSQL-Scale enables Read-Write-Split, the QPS of the leader node is higher compared to when Read-Write-Split is not enabled. This is because a portion of the Read traffic is being routed to the follower node.
- As the number of Sysbench threads continues to increase, MySQL's QPS will reach a bottleneck and cannot continue to grow, but due to contention, latency will continue to increase. However, WeSQL-Scale's QPS can continue to grow, while latency is lower than MySQL.

The above test proves a point: by introducing WeSQL-Scale as a proxy, it can scale horizontally and make use of follower node resources, resulting in improved read and write performance of the cluster.

## MySQL vs WeSQL-Scale (CPU and Memory)



Some Observations:

- In all workload scenarios, the overall memory resource usage of the cluster is similar to MySQL, which demonstrates that wesql-scale only requires a minimal amount of memory.
- At 50% of the read traffic, the CPU resources occupied by the WeSQL-Scale process are equivalent to 1/3 of the total CPU resources.
- In the case of 80% of the read traffic, the CPU resources occupied by the WeSQL-Scale process exceeded half of the total CPU resources, but it also caused the overall QPS to reach double of the original value. This proves that the cost-effectiveness of increasing QPS through read-write-splitting and horizontal scaling with WeSQL-Scale is higher compared to the cost-effectiveness of vertical scaling with MySQL.

# Conclusion

In conclusion, introducing WeSQL-Scale as a proxy can significantly improve the scalability, performance, security, and resilience of a MySQL database. With read-write splitting enabled, WeSQL-Scale can utilize follower node resources, resulting in improved read and write performance of the cluster.

Furthermore, the overhead introduced by WeSQL-Scale is minimal, and its cost-effectiveness for increasing QPS through read-write-splitting and horizontal scaling is higher compared to the cost-effectiveness of vertical scaling with MySQL.