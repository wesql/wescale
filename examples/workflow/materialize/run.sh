#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

vtctlclient --server localhost:15999 Materialize '{
                                      	"workflow": "test_materialize_name_1",
                                      	"source_keyspace": "materialize_source",
                                      	"target_keyspace": "materialize_target",
                                      	"table_settings": [{
                                      		"target_table": "t1_shadow",
                                      		"source_expression": "select * from t1"
                                      	}],
                                      	"cell": "zone1",
                                      	"tablet_types": "REPLICA"
                                      }'
mysql -h127.0.0.1 -P15306 -e 'select * from materialize_target.t1_shadow'

vtctlclient --server localhost:15999 Materialize '{
                                      	"workflow": "test_materialize_name_2",
                                      	"source_keyspace": "materialize_source",
                                      	"target_keyspace": "materialize_target",
                                      	"table_settings": [{
                                      		"target_table": "t2",
                                      		"source_expression": "select * from t1",
                                          "create_ddl": "create table materialize_target.t2 like materialize_source.t1"
                                      	}],
                                      	"cell": "zone1",
                                      	"tablet_types": "REPLICA"
                                      }'
mysql -h127.0.0.1 -P15306 -e 'select * from materialize_target.t2'

vtctlclient --server localhost:15999 Materialize '{
                                      	"workflow": "test_materialize_name_3",
                                      	"source_keyspace": "materialize_source",
                                      	"target_keyspace": "materialize_target",
                                      	"table_settings": [{
                                      		"target_table": "t1",
                                      		"source_expression": "select * from t1",
                                          "create_ddl": "copy"
                                      	}],
                                      	"cell": "zone1",
                                      	"tablet_types": "REPLICA"
                                      }'
mysql -h127.0.0.1 -P15306 -e 'select * from materialize_target.t1'

echo 'listall materialize workflows'
echo 'vtctlclient --server localhost:15999 materialize_target listall'
vtctlclient --server localhost:15999 workflow materialize_target listall

echo 'show materialize workflow: test_materialize_name_1'
echo 'vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 show'
vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 show

echo 'stop materialize workflow: test_materialize_name_1'
echo 'vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 stop'
vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 stop

echo 'start materialize workflow: test_materialize_name_1'
echo 'vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 start'
vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 start

echo 'delete materialize workflow: test_materialize_name_1'
echo 'vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 delete'
vtctlclient --server localhost:15999 Workflow -- materialize_target.test_materialize_1 delete