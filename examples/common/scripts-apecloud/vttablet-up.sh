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

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env-apecloud.sh"

cell=${CELL:-'test'}
uid=$TABLET_UID
mysql_root=${MYSQL_ROOT:-'root'}
mysql_root_passwd=${MYSQL_ROOT_PASSWD:-'1234'}
mysql_port=$[17000 + $uid]
port=$[15100 + $uid]
grpc_port=$[16000 + $uid]
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
tablet_hostname='127.0.0.1'
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

echo "Starting $TABLET_TYPE vttablet for $alias..."

# shellcheck disable=SC2086
vttablet \
 $TOPOLOGY_FLAGS \
 --log_dir $VTDATAROOT/tmp \
 --log_queries_to_file $VTDATAROOT/tmp/$tablet_logfile \
 --tablet-path $alias \
 --tablet_hostname "$tablet_hostname" \
 --init_tablet_type $TABLET_TYPE \
 --health_check_interval 1s \
 --shard_sync_retry_delay 1s \
 --remote_operation_timeout 1s \
 --db_connect_timeout_ms 500 \
 --enable_replication_reporter \
 --backup_storage_implementation file \
 --file_backup_storage_root $VTDATAROOT/backups \
 --port $port \
 --db_port $mysql_port \
 --db_host "$tablet_hostname" \
 --db_allprivs_user "$mysql_root" \
 --db_allprivs_password "$mysql_root_passwd" \
 --db_dba_user "$mysql_root" \
 --db_dba_password "$mysql_root_passwd" \
 --db_app_user "$mysql_root" \
 --db_app_password "$mysql_root_passwd" \
 --db_filtered_user "$mysql_root" \
 --db_filtered_password "$mysql_root_passwd" \
 --grpc_port $grpc_port \
 --service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 --pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
 --vtctld_addr http://$hostname:$vtctld_web_port/ \
 --disable_active_reparents \
 > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

# Block waiting for the tablet to be listening
# Not the same as healthy

for i in $(seq 0 300); do
 curl -I "http://$hostname:$port/debug/status" >/dev/null 2>&1 && break
 sleep 0.1
done

# check one last time
curl -I "http://$hostname:$port/debug/status" || fail "tablet could not be started!"
