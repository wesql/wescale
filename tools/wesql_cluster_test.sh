#!/bin/bash

# run tests against a local cluster
export VTDATAROOT="/tmp/"
source build.env
find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable
go run test.go -docker=false -follow -shard wesql
find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable