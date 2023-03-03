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

# This is an example script that creates a quorum of Etcd servers.

#source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

etcd_port=${ETCD_PORT:-'2379'}
etcd_server=${ETCD_SERVER:-'127.0.0.1:'$etcd_port}

cell=${CELL:-'zone1'}
export ETCDCTL_API=2

# Check that etcd is not already running
#curl "http://${etcd_server}" > /dev/null 2>&1 && fail "etcd is already running. Exiting."

etcd --enable-v2=true --data-dir "${VTDATAROOT}/etcd/"  --listen-client-urls "http://${etcd_server}" --advertise-client-urls "http://${etcd_server}" > "${VTDATAROOT}"/etcd.out 2>&1 &
PID=$!
echo $PID > "${VTDATAROOT}/etcd.pid"
sleep 5

echo "add /vitess/global"
etcdctl --endpoints "http://${etcd_server}" mkdir /vitess/global &

echo "add /vitess/$cell"
etcdctl --endpoints "http://${etcd_server}" mkdir /vitess/$cell &

# And also add the CellInfo description for the cell.
# If the node already exists, it's fine, means we used existing data.
echo "add $cell CellInfo"
set +e
# shellcheck disable=SC2086
vtctl $TOPOLOGY_FLAGS VtctldCommand AddCellInfo \
  --root /vitess/$cell \
  --server-address "${etcd_server}" \
  $cell
set -e

echo "etcd start done..."
wait


