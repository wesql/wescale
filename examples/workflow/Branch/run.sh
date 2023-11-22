#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

vtctlclient --server localhost:15999 Branch -- --source_database branch_source --target_database branch_target --workflow_name branch_test Prepare
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Start

#vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Stop
