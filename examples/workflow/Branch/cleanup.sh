#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

mysql -h127.0.0.1 -P15306 -e 'drop database if exists branch_source'
mysql -h127.0.0.1 -P15306 -e 'drop database if exists branch_target'
mysql -h127.0.0.1 -P15306 -e 'delete from mysql.vreplication where 1=1'
mysql -h127.0.0.1 -P15306 -e 'delete from mysql.branch_jobs where 1=1'
mysql -h127.0.0.1 -P15306 -e 'delete from mysql.branch_table_rules where 1=1'
