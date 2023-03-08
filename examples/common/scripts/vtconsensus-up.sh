#!/bin/bash

script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env.sh"

log_dir="${VTDATAROOT}/tmp"
port=16000

vtconsensus \
  $TOPOLOGY_FLAGS \
  --clusters_to_watch "commerce/0" \
  --refresh_interval 10s \
  --scan_repair_timeout 3s \
  --db_username "root" \
  --db_password "" \
  > "${log_dir}/vtconsensus.out" 2>&1 &

vtconsensus_pid=$!
echo ${vtconsensus_pid} > "${log_dir}/vtconsensus.pid"

echo "vtconsensus is running!
"
