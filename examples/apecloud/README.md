# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Setup environment and aliases
source ../common/env.sh

# Bring up initial cluster and apecloud keyspace
./101_initial_cluster.sh

# Insert and verify data
mysql < ../common/insert_apecloud_data.sql
mysql --table < ../common/select_apecloud_data.sql

# Bring up customer keyspace
./201_customer_tablets.sh

# Initiate move tables
vtctlclient MoveTables -- --source apecloud --tables 'customer,corder' Create customer.apecloud2customer

# Validate
vtctlclient VDiff customer.apecloud2customer

# Cut-over
vtctlclient MoveTables -- --tablet_types=rdonly,replica SwitchTraffic customer.apecloud2customer
vtctlclient MoveTables -- --tablet_types=primary SwitchTraffic customer.apecloud2customer

# Clean-up
vtctlclient MoveTables Complete customer.apecloud2customer

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vtctlclient Reshard -- --source_shards '0' --target_shards '-80,80-' Create customer.cust2cust

# Validate
vtctlclient VDiff customer.cust2cust

# Cut-over
vtctlclient Reshard -- --tablet_types=rdonly,replica SwitchTraffic customer.cust2cust
vtctlclient Reshard -- --tablet_types=primary SwitchTraffic customer.cust2cust

# Down shard 0
./306_down_shard_0.sh
vtctlclient DeleteShard -- --force --recursive customer/0

# Down cluster
./401_teardown.sh
```
