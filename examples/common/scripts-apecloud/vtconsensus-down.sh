#!/bin/bash

# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env-apecloud.sh"

echo "Stopping vtconsensus."
pkill -9 -f 'vtconsensus'

