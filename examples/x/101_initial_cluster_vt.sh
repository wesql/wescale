#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).





source ../common/env.sh

# start topo server
if [ "${TOPO}" = "zk2" ]; then
	CELL=zone1 ../common/scripts/zk-up.sh
elif [ "${TOPO}" = "k8s" ]; then
	CELL=zone1 ../common/scripts/k3s-up.sh
elif [ "${TOPO}" = "consul" ]; then
	CELL=zone1 ../common/scripts/consul-up.sh
else
	CELL=zone1 ../common/scripts/etcd-up.sh
fi

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

# create the schema
#vtctldclient ApplySchema --sql-file create_apecloud_schema.sql _vt || fail "Failed to apply schema for the _vt keyspace"

# create the vschema
#vtctldclient ApplyVSchema --vschema-file vschema_apecloud_initial.json _vt || fail "Failed to apply vschema for the _vt keyspace"

# start vtgate

CELL=zone1 CMD_FLAGS="--vschema_ddl_authorized_users % " ../common/scripts/vtgate-up.sh

# start vtadmin
../common/scripts/vtadmin-up.sh

