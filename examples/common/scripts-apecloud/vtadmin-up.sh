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


script_dir="$(dirname "${BASH_SOURCE[0]:-$0}")"
source "${script_dir}/../env-apecloud.sh"

log_dir="${VTDATAROOT}/tmp"
web_dir="${script_dir}/../../../web/vtadmin"

vtadmin_api_port=14200
vtadmin_web_port=14201

vtadmin \
  --addr ":${vtadmin_api_port}" \
  --http-origin "http://localhost:${vtadmin_web_port}" \
  --http-tablet-url-tmpl "http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  --tracer "opentracing-jaeger" \
  --grpc-tracing \
  --http-tracing \
  --logtostderr \
  --alsologtostderr \
  --rbac \
  --rbac-config="${script_dir}/../vtadmin/rbac.yaml" \
  --cluster "id=local,name=local,discovery=staticfile,discovery-staticfile-path=${script_dir}/../vtadmin/discovery.json,tablet-fqdn-tmpl={{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}" \
  > "${log_dir}/vtadmin-api.out" 2>&1 &

vtadmin_api_pid=$!
echo ${vtadmin_api_pid} > "${log_dir}/vtadmin-api.pid"

echo "\
vtadmin-api is running!
  - API: http://localhost:${vtadmin_api_port}
  - Logs: ${log_dir}/vtadmin-api.out
  - PID: ${vtadmin_api_pid}
"

# As a TODO, it'd be nice to make the assumption that vtadmin-web is already
# installed and built (since we assume that `make` has already been run for
# other Vitess components.)
npm --prefix $web_dir --silent install

REACT_APP_VTADMIN_API_ADDRESS="http://localhost:${vtadmin_api_port}" \
  REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS="true" \
  npm run --prefix $web_dir build

"${web_dir}/node_modules/.bin/serve" --no-clipboard -l $vtadmin_web_port -s "${web_dir}/build" \
  > "${log_dir}/vtadmin-web.out" 2>&1 &

vtadmin_web_pid=$!
echo ${vtadmin_web_pid} > "${log_dir}/vtadmin-web.pid"

echo "\
vtadmin-web is running!
  - Browser: http://localhost:${vtadmin_web_port}
  - Logs: ${log_dir}/vtadmin-web.out
  - PID: ${vtadmin_web_pid}
"
