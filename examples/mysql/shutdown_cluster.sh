#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).





source ../common/env.sh

#../common/scripts/vtadmin-down.sh

../common/scripts/vtorc-down.sh

../common/scripts/vtgate-down.sh

for tablet in 100; do
	if vtctlclient --action_timeout 1s --server localhost:15999 GetTablet zone1-$tablet >/dev/null 2>&1; then
		# The zero tablet is up. Try to shutdown 0-2 tablet + mysqlctl
		for i in 0 1 2; do
			uid=$((tablet + i))
			printf -v alias '%s-%010d' 'zone1' $uid
			echo "Shutting down tablet $alias"
			CELL=zone1 TABLET_UID=$uid ../common/scripts/vttablet-down.sh
			CELL=zone1 TABLET_UID=$uid ../common/scripts/mysqlctl-down.sh
		done
	fi
done

../common/scripts/vtctld-down.sh

CELL=zone1 ../common/scripts/etcd-down.sh

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

disown -a
