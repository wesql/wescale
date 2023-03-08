#!/bin/bash

#script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"

cell=${CELL:-'zone1'}
keyspace=${KEYSPACE:-'commerce'}
shard=${SHARD:-'0'}

#log_dir="${VTDATAROOT}/tmp"
vtconsensusport=${VTCONSENSUS_PORT:-'16000'}
topology_fags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

su vitess <<EOF
exec vtconsensus \
  $topology_fags \
  --clusters_to_watch "$keyspace/$shard" \
  --refresh_interval 10s \
  --scan_repair_timeout 3s \
  --db_username "$MYSQL_ROOT_USER" \
  --db_password "$MYSQL_ROOT_PASSWORD" \
  > $VTDATAROOT/vtconsensus.out 2>&1
EOF
