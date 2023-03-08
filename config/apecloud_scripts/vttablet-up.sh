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
uid="${KB_POD_NAME##*-}"
mysql_root=${MYSQL_ROOT_USER:-'root'}
mysql_root_passwd=${MYSQL_ROOT_PASSWORD:-'123456'}
mysql_port=${MYSQL_PORT:-'3306'}
port=${VTTABLET_PORT:-'15100'}
grpc_port=${VTTABLET_GRPC_PORT:-'16100'}
vtctld_host=${VTCTLD_HOST:-'127.0.0.1'}
vtctld_web_port=${VTCTLD_WEB_PORT:-'15000'}
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
tablet_hostname=$(eval echo \$KB_MYSQL_"$uid"_HOSTNAME)
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

tablet_type=replica
topology_fags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

echo "Starting vttablet for $alias..."

su vitess <<EOF
exec vttablet \
 $topology_fags \
 --log_dir $VTDATAROOT/tmp \
 --log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
 --tablet-path $alias \
 --tablet_hostname "$tablet_hostname" \
 --init_keyspace $keyspace \
 --init_shard $shard \
 --init_tablet_type $tablet_type \
 --health_check_interval 5s \
 --enable_replication_reporter \
 --backup_storage_implementation file \
 --file_backup_storage_root $VTDATAROOT/backups \
 --port $port \
 --db_port $mysql_port \
 --db_host $tablet_hostname \
 --db_allprivs_user $mysql_root \
 --db_allprivs_password $mysql_root_passwd \
 --db_dba_user $mysql_root \
 --db_dba_password $mysql_root_passwd \
 --db_app_user $mysql_root \
 --db_app_password $mysql_root_passwd \
 --db_filtered_user $mysql_root \
 --db_filtered_password $mysql_root_passwd \
 --grpc_port $grpc_port \
 --service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 --pid_file $VTDATAROOT/vttablet.pid \
 --vtctld_addr http://$vtctld_host:$vtctld_web_port/ \
 --disable_active_reparents \
 > $VTDATAROOT/vttablet.out 2>&1
EOF