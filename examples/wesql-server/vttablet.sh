#!/bin/bash

echo "Starting vttablet..."

cell=${CELL:-'zone1'}
uid="${VTTABLET_ID:-'100'}"
mysql_root=${MYSQL_ROOT_USER:-'root'}
mysql_root_passwd=${MYSQL_ROOT_PASSWORD:-'123456'}
mysql_host=${MYSQL_HOST:-'127.0.0.1'}
mysql_port=${MYSQL_PORT:-'3306'}
port=${VTTABLET_PORT:-'15100'}
grpc_port=${VTTABLET_GRPC_PORT:-'16100'}
vtctld_host=${VTCTLD_HOST:-'127.0.0.1'}
vtctld_web_port=${VTCTLD_WEB_PORT:-'15000'}
tablet_hostname=$(eval echo \$KB_"$uid"_HOSTNAME)

printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid
printf -v tablet_logfile 'vttablet_%010d_querylog.txt' $uid

topology_flags=${TOPOLOGY_FLAGS:-'--topo_implementation etcd2 --topo_global_server_address 127.0.0.1:2379 --topo_global_root /vitess/global'}

VTDATAROOT=$VTDATAROOT/vttablet
mkdir -p $VTDATAROOT

vttablet \
  $topology_flags \
  --alsologtostderr \
  --log_dir $VTDATAROOT \
  --log_queries_to_file $VTDATAROOT/$tablet_logfile \
  --tablet-path $alias \
  --tablet_hostname "$tablet_hostname" \
  --init_tablet_type replica \
  --enable_replication_reporter \
  --backup_storage_implementation file \
  --file_backup_storage_root $VTDATAROOT/backups \
  --port $port \
  --db_port $mysql_port \
  --db_host $mysql_host \
  --db_allprivs_user $mysql_root \
  --db_allprivs_password $mysql_root_passwd \
  --db_dba_user $mysql_root \
  --db_dba_password $mysql_root_passwd \
  --db_app_user $mysql_root \
  --db_app_password $mysql_root_passwd \
  --db_filtered_user $mysql_root \
  --db_filtered_password $mysql_root_passwd \
  --grpc_port $grpc_port \
  --service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
  --pid_file $VTDATAROOT/vttablet.pid \
  --vtctld_addr http://$vtctld_host:$vtctld_web_port/ \
  --disable_active_reparents \
  --config_path ./conf
