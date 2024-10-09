#!/bin/bash

echo "正在启动 etcd..."

etcd_port=${ETCD_PORT:-'2379'}
etcd_server=${ETCD_SERVER:-'127.0.0.1'}
cell=${CELL:-'zone1'}
export ETCDCTL_API=2

etcd --enable-v2=true --data-dir "${VTDATAROOT}/etcd/"  \
  --listen-client-urls "http://0.0.0.0:${etcd_port}" \
  --advertise-client-urls "http://0.0.0.0:${etcd_port}"
