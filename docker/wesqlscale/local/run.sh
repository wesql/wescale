#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).




docker run \
-p 14200:14200 \
-p 14201:14201 \
-p 15000:15000 \
-p 15001:15001 \
-p 15991:15991 \
-p 15999:15999 \
-p 16000:16000 \
-p 17100:17100 \
-p 17101:17101 \
-p 17102:17102 \
-p 2379:2379 \
-p 2380:2380 \
--rm \
-it \
earayu/wesql-scale-local



docker run \
-p 14200:14200 \
-p 14201:14201 \
-p 15000:15000 \
-p 15001:15001 \
-p 15991:15991 \
-p 15999:15999 \
-p 16000:16000 \
-p 17100:17100 \
-p 17101:17101 \
-p 17102:17102 \
-p 2379:2379 \
-p 2380:2380 \
--rm \
-it \
--mount type=bind,source=/Users/earayu/Documents/GitHub/apecloud/wesql-scale-2/examples,target=/vt/examples \
earayu/wesql-scale-local
