#!/bin/bash

MYSQL_ROOT_USER=${MYSQL_ROOT_USER:-'root'}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-'1234'}
MYSQL_PORT=${MYSQL_PORT:-'3306'}

UID="${UID##*-}"
VTTABLET_PORT=${VTTABLET_PORT:-'15100'}
VTTABLET_GRPC_PORT=${VTTABLET_GRPC_PORT:-'16100'}

VTGATE_WEB_PORT=${VTGATE_WEB_PORT:-'15001'}
VTGATE_GRPC_PORT=${VTGATE_GRPC_PORT:-'15991'}
VTGATE_MYSQL_PORT=${VTGATE_MYSQL_PORT:-'15306'}

./etcd.sh
./wait-for-service.sh etcd 127.0.0.1 2379

./etcd-post-start.sh

./vtctld.sh
./wait-for-service.sh vtctld 127.0.0.1 15999

./vttablet.sh

./wait-for-service.sh vttablet 127.0.0.1 $VTTABLET_GRPC_PORT
./vtgate.sh

./wait-for-service.sh vtgate 127.0.0.1 $VTGATE_MYSQL_PORT

echo "

------------------------------------------------------------------------

"

echo "VTGate endpoint:
mysql -h127.0.0.1 -P$VTGATE_MYSQL_PORT
"