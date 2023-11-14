#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

vtctlclient --server localhost:15999 MoveTables -- --source movetables_source --tables 'customer,corder' Create movetables_target.source2target

#GetRoutingRules
vtctlclient --server localhost:15999 GetRoutingRules

vtctlclient --server localhost:15999 MoveTables -- Show movetables_target.source2target

vtctlclient --server localhost:15999 MoveTables -- Progress movetables_target.source2target
