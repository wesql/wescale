# 

The goal of this tutorial is to explain how to enable the Read-After-Write-Consistency feature in WeSQL-Scale, in order to ensure that after enabling read-write separation, stale data will not be read.

# Setting through the set command.

If you have already set up a cluster and want to enable Read-After-Write-Consistency, the simplest way is using the MySQL "set" command:

```
# session
set session read_after_write_consistency='SESSION'

# global
set global read_after_write_consistency='SESSION'

```

Currently, WeSQL-Scale supports these consistency levels:

| EVENTUAL | No guarantee of consistency. |
| --- | --- |
| SESSION | Within the same connection, ensure that subsequent read requests can read previous write operations. (recommend) |
| INSTANCE | Within the same instance (VTGate), ensure that subsequent read requests can read previous write operations, even if the read and write requests are not initiated by the same connection. |
| GLOBAL | Within the same WeSQL-Scale cluster, ensure that any read request can read previous write operations. |

# Setting via launch parameters

If you need to set the default value of read_write_splitting_policy, you can pass it as a startup parameter for the vtgate process:
```bash
vtgate \
    # enable read-write-splitting and load balancing
    --read_after_write_consistency session
    # other necessary command line options
    ...
```