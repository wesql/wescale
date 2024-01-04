#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source ../common/env.sh

# start topo server
CELL=zone1 ../common/scripts/docker-etcd-up.sh

# start vtctld
CELL=zone1 vtctld_port=${vtctld_port}  ../common/scripts/docker-vtctld-up.sh

# default jaeger args
rate=${rate:-'0.8'}
args=""
# start tracing
if [ "$tracing" == "on" ]; then
  echo "start all-in-one jaeger backend."
  ../common/scripts/jaeger-up.sh
  args="--tracer opentracing-jaeger --jaeger-agent-host 127.0.0.1:6831 --tracing-sampling-rate ${rate}"
fi

mysql_ports=($(echo $mysql_port | tr "," " "))
index=0
for port in "${mysql_ports[@]}"; do
  port_mapping+="-p ${port}:${port} "
  index=$((index+1))
done

vttablet_ports=($(echo $vttablet_port | tr "," " "))
index=0
for port in "${vttablet_ports[@]}"; do
  port_mapping+="-p ${port}:${port} "
  index=$((index+1))
done

grpc_ports=($(echo $grpc_port | tr "," " "))
index=0
for port in "${grpc_ports[@]}"; do
  port_mapping+="-p ${port}:${port} "
  index=$((index+1))
done
echo "mysql_ports : ${mysql_ports[@]}"
echo "vttablet_ports : ${vttablet_ports[@]}"
echo "grpc_ports : ${grpc_ports[@]}"
# start vttablets for keyspace mysql
for i in 0 1 2; do
	CELL=zone1 TABLET_UID=$((i+100)) mysql_port=${mysql_ports[$i]} ../common/scripts/docker-mysqlctl-up.sh
	JAEGER_ARGS=${args} CELL=zone1 mysql_port=${mysql_ports[$i]} vttablet_port=${vttablet_ports[$i]} grpc_port=${grpc_ports[$i]} TABLET_UID=$((i+100)) ../common/scripts/docker-vttablet-up.sh
done
# set the correct durability policy for the keyspace
vtctldclient --server localhost:${vtctld_port} SetKeyspaceDurabilityPolicy --durability-policy=semi_sync mysql || fail "Failed to set keyspace durability policy on the mysql keyspace"

# start vtorc
../common/scripts/vtorc-up.sh

# Wait for all the tablets to be up and registered in the topology server
# and for a primary tablet to be elected in the shard and become healthy/serving.
wait_for_healthy_shard mysql 0 || exit 1

# start vtgate
JAEGER_ARGS=${args} CELL=zone1 CMD_FLAGS="--vschema_ddl_authorized_users % "  vtgate_port=${vtgate_port} ../common/scripts/docker-vtgate-up.sh

echo "

------------------------------------------------------------------------

"

echo "MySQL endpoint:
mysql -h127.0.0.1 -P${mysql_ports[0]}
mysql -h127.0.0.1 -P${mysql_ports[1]}
mysql -h127.0.0.1 -P${mysql_ports[2]}
"

echo "VTGate endpoint:
mysql -h127.0.0.1 -P${vtgate_port}
"
# start vtadmin
if [ "$vtadmin" == "on" ]; then
  echo "start vtadmin"
  ../common/scripts-apecloud/vtadmin-up.sh
fi

if [ "$tracing" == "on" ]; then
  echo 'jaeger UI endpoint:
http://127.0.0.1:16686/
jaeger client default sampling rate is 0.8.
if you want to modify it, add env var rate=${rate}(0.0~1) to command and restart cluster.

if you want to demand tracing yourself, you should do following things:
step1. `mysql -c` to connect to vtgate. (the '-c' option makes hints in sql being saved)
step2. `select jaeger_span_context();`.
step3. exec sql like  `/*VT_SPAN_CONTEXT=<base64 value>*/ SELECT * from product;` (use base64 value generated in step2)
step4. use TRACEID value generated in step2 to search trace record.'
fi

