#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

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

# start vttablets for keyspace apecloud
for i in 100 101 102; do
	CELL=zone1 TABLET_UID=$i ../common/scripts/mysqlctl-up.sh
	CELL=zone1 KEYSPACE=apecloud TABLET_UID=$i ../common/scripts/vttablet-up.sh
done

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync apecloud || fail "Failed to set keyspace durability policy on the apecloud keyspace"

# start vtorc
../common/scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard apecloud 0 || exit 1

# create the schema
vtctldclient ApplySchema --sql-file create_apecloud_schema.sql apecloud || fail "Failed to apply schema for the apecloud keyspace"

# create the vschema
vtctldclient ApplyVSchema --vschema-file vschema_apecloud_initial.json apecloud || fail "Failed to apply vschema for the apecloud keyspace"

# start vtgate

CELL=zone1 CMD_FLAGS="--dbddl_plugin apeCloud --vschema_ddl_authorized_users % " ../common/scripts/vtgate-up.sh

# start vtadmin
../common/scripts/vtadmin-up.sh

