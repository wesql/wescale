#!/bin/bash

CONTAINER_NAME="mysql-server"
MYSQL_ROOT_PASSWORD="passwd"
MYSQL_ROOT_HOST="%"
HOST_PORT=3306
CONTAINER_PORT=3306
DATA_DIR="$(pwd)/vtdataroot/mysql"
CONFIG_FILE="$DATA_DIR/my.cnf"
IMG="mysql/mysql-server:8.0.32"

# 获取当前用户的 UID 和 GID
USER_ID=$(id -u)
GROUP_ID=$(id -g)

# 创建数据目录和日志目录（如果不存在）
mkdir -p "$DATA_DIR/data"
mkdir -p "$DATA_DIR/log"

# 创建 my.cnf（如果不存在）
if [ ! -f "$CONFIG_FILE" ]; then
cat <<EOL > "$CONFIG_FILE"
[mysqld]
port=3306
bind-address=0.0.0.0
log-bin=binlog
gtid_mode=ON
enforce_gtid_consistency=ON
log_replica_updates=ON
binlog_format=ROW
datadir=/data/mysql/data
log-error=/data/mysql/log/mysqld-error.log
EOL
fi

# 设置目录所有权
chown -R $USER_ID:$GROUP_ID "$DATA_DIR"

# 运行 Docker 容器
docker run -itd --name $CONTAINER_NAME \
  --user $USER_ID:$GROUP_ID \
  -p $HOST_PORT:$CONTAINER_PORT \
  -v "$DATA_DIR":/data/mysql \
  -v "$CONFIG_FILE":/etc/my.cnf \
  -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
  -e MYSQL_ROOT_HOST="$MYSQL_ROOT_HOST" \
  $IMG