#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

echo "apecloud mysql docker remove ..."
docker rm mysql-server1 -f
docker rm mysql-server2 -f
docker rm mysql-server3 -f
docker rm mysql-server -f

