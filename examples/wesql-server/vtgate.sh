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

original_config="$config_path/../bak/vtgate.cnf"
temp_config="$config_path/vtgate.cnf"

# 检查原始配置文件是否存在
if [ ! -f "$original_config" ]; then
    echo "配置文件 $original_config 不存在。请确保 CONFIG_PATH 设置正确并且配置文件存在。"
    exit 1
fi

# 拷贝原始配置文件到临时文件
cp "$original_config" "$temp_config"

printf "\n\n" >> "$temp_config"

# 解析 topology_flags 并转换为 key=value 形式追加到临时配置文件
# 使用数组来正确处理带有空格的值
IFS=' ' read -r -a flags_array <<< "$topology_flags"

i=0
while [ $i -lt ${#flags_array[@]} ]; do
    flag="${flags_array[$i]}"
    if [[ "$flag" == --* ]]; then
        key="${flag#--}"
        # 检查是否为 --key=value 形式
        if [[ "$key" == *"="* ]]; then
            key="${key%%=*}"
            value="${flag#*=}"
        else
            # 检查下一个参数是否存在且不是另一个 flag
            next_index=$((i + 1))
            if [ $next_index -lt ${#flags_array[@]} ] && [[ "${flags_array[$next_index]}" != --* ]]; then
                value="${flags_array[$next_index]}"
                i=$next_index
            else
                # 布尔值标志
                value=true
            fi
        fi
        echo "$key=$value" >> "$temp_config"
    fi
    i=$((i + 1))
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

# 启动 vtgate，仅使用 --config_path 参数
vtgate \
  --config_path "$config_path"
