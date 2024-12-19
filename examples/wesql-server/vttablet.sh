#!/bin/bash

echo "Starting vttablet..."

cell=${CELL:-'zone1'}
uid="${VTTABLET_ID:-0}"
mysql_root=${MYSQL_ROOT_USER:-'root'}
mysql_root_passwd=${MYSQL_ROOT_PASSWORD:-'passwd'}
mysql_host=${MYSQL_HOST:-'127.0.0.1'}
mysql_port=${MYSQL_PORT:-'3306'}
port=${VTTABLET_PORT:-'15100'}
grpc_port=${VTTABLET_GRPC_PORT:-'16100'}
vtctld_host=${VTCTLD_HOST:-'127.0.0.1'}
vtctld_web_port=${VTCTLD_WEB_PORT:-'15000'}
tablet_hostname='127.0.0.1'
config_path=${CONFIG_PATH}

printf -v alias '%s-%010d' "$cell" "$uid"
printf -v tablet_dir 'vt_%010d' "$uid"
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' "$uid"

topology_flags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

VTDATAROOT=$VTDATAROOT/vttablet
mkdir -p "$VTDATAROOT"

# 定义配置文件路径
original_config="$config_path/vttablet.cnf"
temp_config="$config_path/vttablet.cnf.tmp"

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
log_queries_to_file=$VTDATAROOT/$tablet_logfile
tablet_path=$alias
tablet_hostname=$tablet_hostname
init_tablet_type=replica
enable_replication_reporter=true
backup_storage_implementation=file
file_backup_storage_root=$VTDATAROOT/backups
port=$port
db_port=$mysql_port
db_host=$mysql_host
db_allprivs_user=$mysql_root
db_allprivs_password=$mysql_root_passwd
db_dba_user=$mysql_root
db_dba_password=$mysql_root_passwd
db_app_user=$mysql_root
db_app_password=$mysql_root_passwd
db_filtered_user=$mysql_root
db_filtered_password=$mysql_root_passwd
grpc_port=$grpc_port
service_map=grpc-queryservice,grpc-tabletmanager,grpc-updatestream
pid_file=$VTDATAROOT/vttablet.pid
vtctld_addr=http://$vtctld_host:$vtctld_web_port/
disable_active_reparents=true
EOL

# 替换原始配置文件
mv "$temp_config" "$original_config"

# 启动 vttablet，仅使用 --config_path 参数
vttablet \
  --config_path "$config_path"
