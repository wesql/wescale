#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).



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

source ../common/env-apecloud.sh

# start topo server
CELL=zone1 ../common/scripts-apecloud/etcd-up.sh

# start vtctld
CELL=zone1 ../common/scripts-apecloud/vtctld-up.sh

# start vttablets for keyspace commerce
for i in 11 12 13; do
	CELL=zone1 TABLET_UID=$i ../common/scripts-apecloud/apecloudmysql-up.sh
	CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ../common/scripts-apecloud/vttablet-up.sh
done

# set the correct durability policy for the keyspace
vtctldclient --server localhost:15999 SetKeyspaceDurabilityPolicy --durability-policy=semi_sync commerce || fail "Failed to set keyspace durability policy on the commerce keyspace"

# start vtconsensus for apecloud mysql
CELL=zone1 ../common/scripts-apecloud/vtconsensus-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
echo "wait for healthy shard for a primary tablet to be elected"
wait_for_healthy_shard commerce 0 || exit 1

# create the schema
vtctldclient ApplySchema --sql-file create_commerce_schema.sql commerce || fail "Failed to apply schema for the commerce keyspace"

# create the vschema
vtctldclient ApplyVSchema --vschema-file vschema_commerce_initial.json commerce || fail "Failed to apply vschema for the commerce keyspace"

# start vtgate
CELL=zone1 ../common/scripts-apecloud/vtgate-up.sh

# start vtadmin
../common/scripts-apecloud/vtadmin-up.sh

echo "vitess client connection: mysql -uroot"

