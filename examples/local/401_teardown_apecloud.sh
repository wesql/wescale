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

# We should not assume that any of the steps have been executed.
# This makes it possible for a user to cleanup at any point.

source ../common/env-apecloud.sh

../common/scripts-apecloud/vtadmin-down.sh

../common/scripts-apecloud/vtgate-down.sh

../common/scripts-apecloud/vtconsensus-down.sh

for tablet in 1 2 3 11 12; do
		CELL=zone1 TABLET_UID=$tablet ../common/scripts-apecloud/vttablet-down.sh
		TABLET_UID=$tablet ../common/scripts-apecloud/apecloudmysql-down.sh
done

../common/scripts-apecloud/vtctld-down.sh

CELL=zone1 ../common/scripts-apecloud/etcd-down.sh

# pedantic check: grep for any remaining processes

if [ -n "$VTDATAROOT" ]; then
	if pgrep -f -l "$VTDATAROOT" >/dev/null; then
		echo "ERROR: Stale processes detected! It is recommended to manuallly kill them:"
		pgrep -f -l "$VTDATAROOT"
	else
		echo "All good! It looks like every process has shut down"
	fi

	# shellcheck disable=SC2086
	rm -r ${VTDATAROOT:?}/*
fi

docker network rm my_wesqlscale_network

disown -a
