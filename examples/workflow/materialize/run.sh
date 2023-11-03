#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

vtctlclient --server localhost:15999 Materialize '{
                                      	"workflow": "test_materialize_name_1",
                                      	"source_keyspace": "materialize_source",
                                      	"target_keyspace": "materialize_target",
                                      	"table_settings": [{
                                      		"target_table": "t1",
                                      		"source_expression": "select * from t1"
                                      	}],
                                      	"cell": "zone1",
                                      	"tablet_types": "REPLICA"
                                      }'