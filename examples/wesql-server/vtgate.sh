#!/bin/bash

cell=${CELL:-'zone1'}
web_port=${VTGATE_WEB_PORT:-'15001'}
grpc_port=${VTGATE_GRPC_PORT:-'15991'}
mysql_server_port=${VTGATE_MYSQL_PORT:-'15306'}
mysql_server_socket_path="/tmp/mysql.sock"

echo "starting vtgate."
su vitess
exec vtgate \
  $TOPOLOGY_FLAGS \
  --alsologtostderr \
  --log_dir $VTDATAROOT \
  --log_queries_to_file $VTDATAROOT/vtgate_querylog.txt \
  --port $web_port \
  --grpc_port $grpc_port \
  --mysql_server_port $mysql_server_port \
  --mysql_server_socket_path $mysql_server_socket_path \
  --cell $cell \
  --cells_to_watch $cell \
  --tablet_types_to_wait PRIMARY,REPLICA \
  --service_map 'grpc-vtgateservice' \
  --pid_file $VTDATAROOT/vtgate.pid \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &