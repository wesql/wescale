#!/bin/bash

CONTAINER_NAME="mysql-server"
MYSQL_ROOT_PASSWORD="passwd"
MYSQL_ROOT_HOST="%"
HOST_PORT=3306
CONTAINER_PORT=3306
IMG="mysql/mysql-server:8.0.32"

docker run -itd --name $CONTAINER_NAME \
  -p $HOST_PORT:$CONTAINER_PORT \
  -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
  -e MYSQL_ROOT_HOST="$MYSQL_ROOT_HOST" \
  -e MYSQL_LOG_CONSOLE=true \
  $IMG \
  --bind-address=0.0.0.0 \
  --port=3306 \
  --log-bin=binlog \
  --gtid_mode=ON \
  --enforce_gtid_consistency=ON \
  --log_replica_updates=ON \
  --binlog_format=ROW