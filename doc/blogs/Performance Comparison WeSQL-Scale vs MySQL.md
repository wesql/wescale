# Performance Comparison: WeSQL-Scale vs MySQL

WeSQL-Scale is a database proxy that is compatible with MySQL. It supports read-write splitting without stale reads, connection pooling, and transparent failover. These features work together to improve application scalability, performance, security, and resilience. In this article, we will compare the performance of WeSQL-Scale and MySQL, and examine how these features affect throughput and latency. Let's get started!

For those in a hurry, here are the main findings:

- Adding WeSQL-Scale as a proxy for MySQL may result in some extra network round trips, but as the workload increases, it can achieve the same throughput and latency as a direct connection to MySQL. 
- Due to connection multiplexing, WeSQL-Scale can maintain stable high throughput during high workloads, while MySQL's performance may be reduced due to excessive connection occupying memory in the buffer pool.
- When the read-write-split feature is activated, WeSQL-Scale demonstrates linear throughput growth even after MySQL reaches its bottleneck, as the number of pressure testing threads gradually increases.
- The additional memory consumption of WeSQL-Scale can be considered negligible compared to MySQL.
- The CPU overhead introduced by WeSQL-Scale accounts for only about 1/3 of the total resource consumption. The cost-effectiveness of increasing QPS through read-write-splitting and horizontal scaling with WeSQL-Scale is higher compared to the cost-effectiveness of vertical scaling with MySQL.

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

- When the sysbench thread count is low, WeSQL-Scale's throughput is lower than when directly connecting to MySQL. This is due to the introduction of additional network round trips. Deploying the application with VTGate can mitigate this issue. In most cases, the SQL queries in the business logic are more complex than SysBench, so the impact of the network is relatively lower in reality. 
- As the number of threads increases, the performance and throughput of WeSQL-Scale begins to reach, and even exceed, that of directly connecting to MySQL. This is because WeSQL-Scale supports connection multiplexing. With the same throughput, it creates fewer connections on the MySQL side, allowing MySQL to allocate more memory to the buffer pool.

During the testing process, we found that MySQL couldn't handle too many sysbench threads due to the limitation of max_connection. We had to modify the max_connection parameter in order to test with 200 sysbench threads. However, with max_connections set to 100 in MySQL, WeSQL-Scale can handle over 5000 sysbench threads (we are unable to increase the number of sysbench threads further due to memory constraints on the server).

The above test proves a point: introducing WeSQL-Scale as a proxy will not affect the throughput of the database. In high workload scenarios, it can even maintain throughput without decreasing as connections increase. WeSQL-Scale can help you avoid errors like 'too many connections'.

### How about the resources introduced by WeSQL-Scale

| Threads | WeSQL-Scale CPU (m) | WeSQL-Scale Memory (Mi) | MySQL CPU (m) | MySQL Memory (Mi) |
| --- |---------------------|-------------------------|---------------|---------------|
| 4 | 4916.74             | 30096.41                | 8036.75       | 30146.91      |
| 8 | 10913.25            | 30124.00                | 9685.58       | 30154.8       |
| 16 | 13206.75            | 30140.25                | 10240.5       | 30146.41      |
| 25 | 13961.66            | 30126.58                | 9692          | 30161         |
| 50 | 13820.16            | 30131.58                | 9895.25       | 30093.25      |
| 75 | 14413.75            | 30116.08                | 10398.75      | 30132.91      |
| 100 | 14553.08            | 30154.91                | 10446.41      | 30128.8       |
| 125 | 14511.24            | 30139.00                | 10440.58      | 30142.68      |
| 150 | 14942.08            | 30146.75                | 10859.91      | 30177.08      |
| 175 | 15190.50            | 30134.91                | 10797.58      | 30037         |
| 200 | 15280.75            | 30119.50                | 11137.25      | 30139.8       |

![read_write_split_disable_cpu_memory.png](images%2Fread_write_split_disable_cpu_memory.png)

During our SysBench testing, we simultaneously measured the resource usage of the two:
- The additional memory consumption of WeSQL-Scale can be considered negligible compared to MySQL.
- WeSQL-Scale is expected to utilize approximately 1/3 more CPU resources.

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
- As the number of Sysbench threads continues to increase, MySQL's QPS will reach a bottleneck and cannot continue to grow, but due to contention, latency will continue to increase. 
- WeSQL-Scale's QPS can continue to grow, while latency is lower than MySQL, the QPS has almost doubled compared to before.

The above test proves a point: by introducing WeSQL-Scale as a proxy, it can scale horizontally and make use of follower node resources, resulting in improved read and write performance of the cluster.

### How about the resources introduced by WeSQL-Scale

| Threads | WeSQL-Scale CPU (m) | WeSQL-Scale Memory (Mi) | MySQL CPU (m) | MySQL Memory (Mi) |
| --- | --- | --- | --- | --- |
| 4 | 9313.58 | 30147.91 | 8507.58 | 30215.75 |
| 8 | 12382.00 | 30123.74 | 10054.8 | 30120.8 |
| 16 | 14923.64 | 30136 | 10851.99 | 30120.75 |
| 25 | 15933.00 | 30090.1 | 10491 | 30188.91 |
| 50 | 16377.66 | 30119.25 | 9986.58 | 30179 |
| 75 | 15982.91 | 30193.91 | 9882.91 | 30168.91 |
| 100 | 16423.68 | 30179.58 | 10180 | 30198.58 |
| 125 | 16628.91 | 30182 | 10516.58 | 30208.75 |
| 150 | 16805.75 | 30184.68 | 10410.66 | 30199.41 |
| 175 | 17098.58 | 30182.49 | 10584.91 | 30189.58 |
| 200 | 17211.50 | 30202.25 | 10745.66 | 30185.91 |

![read_write_split_enable_cpu_memory.png](images%2Fread_write_split_enable_cpu_memory.png)

- In all workload scenarios, the overall memory resource usage of the cluster is similar to MySQL, which demonstrates that wesql-scale only requires a minimal amount of memory.
- Even though the QPS has almost doubled, the additional CPU introduced by WeSQL-Scale is only 70% of MySQL's. This indicates that the cost-effectiveness of horizontal scaling through the read-write separation feature is relatively high.

# Conclusion

In conclusion, introducing WeSQL-Scale as a proxy can significantly improve the scalability, performance, security, and resilience of a MySQL database. With read-write-splitting enabled, WeSQL-Scale can utilize follower node resources, resulting in improved read and write performance of the cluster.
</br>
Furthermore, the overhead introduced by WeSQL-Scale is minimal, and its cost-effectiveness for increasing QPS through read-write-splitting and horizontal scaling is higher compared to the cost-effectiveness of vertical scaling with MySQL.

