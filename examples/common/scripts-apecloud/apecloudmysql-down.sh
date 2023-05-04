#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

echo "apecloud mysql docker stop ..."
docker stop mysql-server1
docker stop mysql-server2
docker stop mysql-server3
echo "apecloud mysql docker remove ..."
docker rm mysql-server1
docker rm mysql-server2
docker rm mysql-server3

