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

#source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

cell=${CELL:-'zone1'}
keyspace=${KEYSPACE:-'commerce'}
shard=${SHARD:-'0'}
uid=$TABLET_UID
mysql_port=3306
port=15010
grpc_port=16010
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
tablet_hostname=$KB_HOSTIP
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

tablet_type=replica
topology_fags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

echo "Starting vttablet for $alias..."

vttablet \
 $topology_fags \
 --log_dir $VTDATAROOT/tmp \
 --log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
 --tablet-path $alias \
 --tablet_hostname "$tablet_hostname" \
 --init_keyspace $keyspace \
 --init_shard $shard \
 --init_tablet_type $tablet_type \
 --health_check_interval 5s \
 --backup_storage_implementation file \
 --file_backup_storage_root $VTDATAROOT/backups \
 --port $port \
 --db_port $mysql_port \
 --db_host $KB_MYSQL_0_HOSTNAME= \
 --grpc_port $grpc_port \
 --service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 --pid_file $VTDATAROOT/vttablet.pid \
 --vtctld_addr http://$hostname:$vtctld_web_port/ \
 --disable_active_reparents \
 > $VTDATAROOT/vttablet.out 2>&1 &
## Block waiting for the tablet to be listening
## Not the same as healthy
#
#for i in $(seq 0 300); do
# curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
# sleep 0.1
#done
#
## check one last time
#curl -I "http://$hostname:$port/debug/status" || fail "tablet could not be started!"
