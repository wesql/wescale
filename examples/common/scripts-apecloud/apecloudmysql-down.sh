#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
server_name="mysql-server$TABLET_UID"
echo "Removing apecloud mysql docker $server_name"
docker rm $server_name -f &>/dev/null

