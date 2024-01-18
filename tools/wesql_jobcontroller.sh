#!/bin/bash
export VTDATAROOT="/tmp/"
source build.env
find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable
go run test.go -docker=false -follow -shard jobcontroller
find `pwd`/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable