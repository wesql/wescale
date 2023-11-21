#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

vtctlclient --server localhost:15999 Branch -- --source branch_source --all --special_rules '{
  "special_rules": [
    {
      "source_table": "corder",
      "filter_rule": "select order_id as order_id,customer_id as customer_id,sku as sku,122 as price from corder",
      "create_ddl": "create table branch_target.corder like movetables_source.corder"
    }
  ]
}' Create branch_target.source2target
