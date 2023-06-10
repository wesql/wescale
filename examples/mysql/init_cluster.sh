#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).





source ../common/env.sh

# start topo server
CELL=zone1 ../common/scripts/etcd-up.sh

# start vtctld
CELL=zone1 ../common/scripts/vtctld-up.sh

# start vttablets for keyspace _vt
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	CELL=zone1 TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync _vt || fail "Failed to set keyspace durability policy on the _vt keyspace"

# start vtorc
../common/scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard _vt 0 || exit 1

# start vtgate
CELL=zone1 CMD_FLAGS="--vschema_ddl_authorized_users % " ../common/scripts/vtgate-up.sh

echo "

------------------------------------------------------------------------

"

echo "MySQL endpoint:
mysql -h127.0.0.1 -P17100
mysql -h127.0.0.1 -P17101
mysql -h127.0.0.1 -P17102
"

echo "VTGate endpoint:
mysql -h127.0.0.1 -P15306
"

# if env var debug=on, kill the vtgate process and vttablet processes
if [ "$debug" == "on" ]; then
  killall vtgate vttablet
  echo "vtgate and vttablet processes killed"
fi

# start vtadmin
#../common/scripts/vtadmin-up.sh

