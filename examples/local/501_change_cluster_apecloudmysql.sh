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

# this script brings up etcd and all the vitess components
# required for a single shard deployment.

source ../common/env-apecloud.sh

echo "Staring upgrade learner to follower, change tablet type rdonly to replica ..."

TABLETS_UID=(12)
for i in ${TABLETS_UID[@]}; do
  cluster_info="192.168.0."$[$i + 1]":13306"
	# upgrade learner to follower
  echo "Upgrade learner $cluster_info to follower"
  mysql -h127.0.0.1 -P15306  -e "call dbms_consensus.upgrade_learner('$cluster_info');" >> ${VTDATAROOT}/tmp/upgrade_learner_error.log 2>&1
  # change tablet type rdonly to replica
  printf -v tabletalias 'zone1-%010d' $i
  echo "Change $tabletalias tablet type rdonly to replica"
  vtctldclient --server localhost:15999  ChangeTabletType $tabletalias replica
done

echo "Staring downgrade follower to learner, change tablet type replica to rdonly ..."

TABLETS_UID=(11)
for i in ${TABLETS_UID[@]}; do
  cluster_info="192.168.0."$[$i + 1]":13306"
  echo "Upgrade learner $cluster_info to follower"
  mysql -h127.0.0.1 -P15306  -e "call dbms_consensus.downgrade_follower('$cluster_info');" >> ${VTDATAROOT}/tmp/upgrade_learner_error.log 2>&1
  printf -v tabletalias 'zone1-%010d' $i
  echo "Change $tabletalias tablet type replica to rdonly"
  vtctldclient --server localhost:15999  ChangeTabletType $tabletalias rdonly
done

echo ""
echo "WeSQL-Scale cluster change done"

