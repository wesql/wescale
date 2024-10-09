#!/bin/bash

# 设置默认值
export MYSQL_ROOT_USER=${MYSQL_ROOT_USER:-'root'}
export MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-'passwd'}
export MYSQL_PORT=${MYSQL_PORT:-'3306'}

export UID="${UID:-'100'}"
export VTTABLET_PORT=${VTTABLET_PORT:-'15100'}
export VTTABLET_GRPC_PORT=${VTTABLET_GRPC_PORT:-'16100'}

export VTGATE_WEB_PORT=${VTGATE_WEB_PORT:-'15001'}
export VTGATE_GRPC_PORT=${VTGATE_GRPC_PORT:-'15991'}
export VTGATE_MYSQL_PORT=${VTGATE_MYSQL_PORT:-'15306'}

export VTDATAROOT=${VTDATAROOT:-$(pwd)/vtdataroot}

# 定义日志目录
LOG_DIR="$VTDATAROOT/logs"
mkdir -p "$LOG_DIR"

# 定义一个函数来捕获脚本退出信号，清理后台进程
cleanup() {
  echo "正在清理后台进程..."
  kill $(jobs -p)
  exit 0
}

# 捕获退出信号
trap cleanup SIGINT SIGTERM EXIT

# 定义一个函数，为输出添加前缀
run_with_prefix() {
  prefix=$1
  shift
  "$@" 2>&1 | sed "s/^/[$prefix] /" | tee "$LOG_DIR/$prefix.log" &
}

echo "初始化单节点集群..."

# 启动 etcd
echo "启动 etcd..."
run_with_prefix "etcd" ./etcd.sh
echo "等待 etcd 服务启动..."
./wait-for-service.sh etcd 127.0.0.1 2379
echo "etcd 已启动。"

# etcd 后续配置
echo "执行 etcd 后续配置..."
./etcd-post-start.sh

# 启动 vtctld
echo "启动 vtctld..."
run_with_prefix "vtctld" ./vtctld.sh
echo "等待 vtctld 服务启动..."
./wait-for-service.sh vtctld 127.0.0.1 15999
echo "vtctld 已启动。"

# 启动 vttablet
echo "启动 vttablet..."
run_with_prefix "vttablet" ./vttablet.sh
echo "等待 vttablet 服务启动..."
./wait-for-service.sh vttablet 127.0.0.1 $VTTABLET_GRPC_PORT
echo "vttablet 已启动。"

# 启动 vtgate
echo "启动 vtgate..."
run_with_prefix "vtgate" ./vtgate.sh
echo "等待 vtgate 服务启动..."
./wait-for-service.sh vtgate 127.0.0.1 $VTGATE_MYSQL_PORT
echo "vtgate 已启动。"

echo "
------------------------------------------------------------------------
"

echo "VTGate 访问入口：
mysql -h127.0.0.1 -P$VTGATE_MYSQL_PORT
"

# 保持脚本运行，以便捕获退出信号
wait
