#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env-apecloud.sh"

log_dir="${VTDATAROOT}/tmp"
port=16000

vtconsensus \
  $TOPOLOGY_FLAGS \
  --refresh_interval 1s \
  --scan_repair_timeout 1s \
  --log_dir ${log_dir} \
  --db_username "root" \
  --db_password "" \
  > "${log_dir}/vtconsensus.out" 2>&1 &

vtconsensus_pid=$!
echo ${vtconsensus_pid} > "${log_dir}/vtconsensus.pid"

echo "vtconsensus is running!
"
