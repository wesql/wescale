#!/bin/bash

echo 'Starting vreplication unit test'

source build.env

go test vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication
go test vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer

