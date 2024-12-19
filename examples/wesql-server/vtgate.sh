#!/bin/bash

echo "Starting vtgate..."

cell=${CELL:-'zone1'}
web_port=${VTGATE_WEB_PORT:-'15001'}
grpc_port=${VTGATE_GRPC_PORT:-'15991'}
mysql_server_port=${VTGATE_MYSQL_PORT:-'15306'}
mysql_server_socket_path="/tmp/mysql.sock"
config_path=${CONFIG_PATH}

topology_flags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

VTDATAROOT=$VTDATAROOT/vtgate
mkdir -p "$VTDATAROOT"

original_config="$config_path/vtgate.cnf"
temp_config="$config_path/vtgate.cnf.tmp"

# 检查原始配置文件是否存在
if [ ! -f "$original_config" ]; then
    echo "配置文件 $original_config 不存在。请确保 CONFIG_PATH 设置正确并且配置文件存在。"
    exit 1
fi

# 拷贝原始配置文件到临时文件
cp "$original_config" "$temp_config"

# 将 topology_flags 转换为 key=value 并追加到临时配置文件
for flag in $topology_flags; do
    # 去掉前缀 --
    key=${flag%%=*}
    key=${key#--}
    value=${flag#*=}
    # 如果没有 '=', 则为布尔值
    if [ "$key" = "$flag" ]; then
        value=true
    fi
    echo "$key=$value" >> "$temp_config"
done

# 追加其他标志到临时配置文件
cat <<EOL >> "$temp_config"
alsologtostderr=true
log_dir=$VTDATAROOT
log_queries_to_file=$VTDATAROOT/vtgate_querylog.txt
port=$web_port
grpc_port=$grpc_port
mysql_server_port=$mysql_server_port
mysql_server_socket_path=$mysql_server_socket_path
cell=$cell
cells_to_watch=$cell
service_map=grpc-vtgateservice
pid_file=$VTDATAROOT/vtgate.pid
EOL

# 替换原始配置文件
mv "$temp_config" "$original_config"

# 启动 vtgate，仅使用 --config_path 参数
vtgate \
  --config_path "$config_path"
