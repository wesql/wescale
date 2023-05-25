#!/bin/bash

# run tests against a local cluster
export VTDATAROOT="/tmp/"
source build.env
go run test.go -docker=false -follow  -db-flavor=wesql -shard vtconcensus