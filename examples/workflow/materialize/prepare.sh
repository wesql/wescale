#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"


mysql -h127.0.0.1 -P15306 -e 'create database if not exists materialize_source'
mysql -h127.0.0.1 -P15306 -e 'create database if not exists materialize_target'

mysql -h127.0.0.1 -P15306 -e 'create table materialize_source.t1(
                              	c1 bigint primary key auto_increment,
                              	c2 int not null default 1,
                              	c3 int not null default 0
                              );
'
sleep 1
mysql -h127.0.0.1 -P15306 -e 'create table materialize_target.t1_shadow like materialize_source.t1'
sleep 1

mysql -h127.0.0.1 -P15306 -e 'insert into materialize_source.t1 values (null, 1, 1);'
mysql -h127.0.0.1 -P15306 -e 'insert into materialize_source.t1 values (null, 1, 1);'
mysql -h127.0.0.1 -P15306 -e 'insert into materialize_source.t1 values (null, 1, 1);'

