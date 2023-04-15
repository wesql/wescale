#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env-apecloud.sh"
cell=${CELL:-'test'}
uid=$TABLET_UID
tablets_uid=${TABLETS_UID[@]}
# container ip range [192.168.0.1, 192.168.0.2, 192.168.0.3 ....]
container_host="192.168.0."$[$uid + 1]
cur_path=$(dirname $(readlink -f "$0"))
port=$[17000 + $uid]
idx=$uid
printf -v alias '%s-%010d' $cell $uid
printf -v tablet_dir 'vt_%010d' $uid

if [[ "$TABLET_TYPE" = "replica" ]]; then
  #generate cluster_info
  for i in ${tablets_uid[@]}; do
    cluster_info=$cluster_info"192.168.0."$[$i + 1]":13306;"
  done
  cluster_info=$(echo "$cluster_info" | sed 's/.$/@/')
else
  cluster_info="192.168.0."$[$uid + 1]":13306"
  idx=
fi

mkdir -p $VTDATAROOT/backups

echo "Starting MySQL for tablet $alias..."
action="init"

if [ -d $VTDATAROOT/$tablet_dir ]; then
 echo "Resuming from existing vttablet dir:"
 echo "    $VTDATAROOT/$tablet_dir"
 action='start'
fi

echo "start apecloud mysql docker mysql-server$idx"
docker run -itd  \
    --name mysql-server$idx \
    --network my_wesqlscale_network \
    --ip $container_host \
    -p $port:3306     \
    -v ${cur_path}/../../../config/apecloud_mycnf:/etc/mysql/conf.d \
    -v ${cur_path}/../../../config/apecloud_local_scripts:/docker-entrypoint-initdb.d/    \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
    -e MYSQL_INIT_CONSENSUS_PORT=13306 \
    -v $VTDATAROOT/$tablet_dir:/mysql \
    -e CLUSTER_ID=1 \
    -e CLUSTER_INFO="$cluster_info"$idx \
    apecloud/apecloud-mysql-server:8.0.30-5.alpha2.20230105.gd6b8719.2

echo "apecloud mysql instance client connection: mysql -h$hostname -uroot -P$port"

