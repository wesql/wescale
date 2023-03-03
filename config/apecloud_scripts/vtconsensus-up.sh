#!/bin/bash

#script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"

cell=${CELL:-'zone1'}
keyspace=${KEYSPACE:-'commerce'}
shard=${SHARD:-'0'}

#log_dir="${VTDATAROOT}/tmp"
port=16000

vtconsensus \
  $TOPOLOGY_FLAGS \
  --clusters_to_watch "$keyspace/$shard" \
  --refresh_interval 10s \
  --scan_repair_timeout 3s \
  > $VTDATAROOT/vtconsensus.out 2>&1 &
