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

# This is an example script that starts vtctld.

#source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

cell=${CELL:-'zone1'}
grpc_port=${VTCTLD_GRPC_PORT:-'15999'}
vtctld_web_port=${VTCTLD_WEB_PORT:-'15000'}
topology_fags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

echo "Starting vtctld..."
# shellcheck disable=SC2086
su vitess <<EOF
exec vtctld \
 $topology_fags \
 --cell $cell \
 --service_map 'grpc-vtctl,grpc-vtctld' \
 --backup_storage_implementation file \
 --file_backup_storage_root $VTDATAROOT/backups \
 --log_dir $VTDATAROOT \
 --port $vtctld_web_port \
 --grpc_port $grpc_port \
 --pid_file $VTDATAROOT/vtctld.pid \
  > $VTDATAROOT/vtctld.out 2>&1
EOF
