# Copyright ApeCloud, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MAKEFLAGS = -s
GIT_STATUS := $(shell git status --porcelain)

IMG ?= registry.cn-hangzhou.aliyuncs.com/apecloud/apecloud-mysql-scale
VERSION ?= latest
BUILDX_ARGS ?=
BUILDX_PLATFORMS ?= linux/amd64,linux/arm64

ifndef GOARCH
export GOARCH=$(go env GOARCH)
endif

# This is where Go installs binaries when you run `go install`. By default this
# is $GOPATH/bin. It is better to try to avoid setting this globally, because
# Go will complain if you try to cross-install while this is set.
#
# GOBIN=

ifndef GOOS
export GOOS=$(go env GOOS)
endif

# GOPATH is the root of the Golang installation. `bin` is nested under here. In
# development environments, this is usually $HOME/go. In production and Docker
# environments, this is usually /go.
ifndef GOPATH
export GOPATH=$(go env GOROOT)
endif

# This governs where Vitess binaries are installed during `make install` and
# `make cross-install`. Typically for production builds we set this to /vt.
# PREFIX=

export REWRITER=go/vt/sqlparser/rewriter.go

# Disabled parallel processing of target prerequisites to avoid that integration tests are racing each other (e.g. for ports) and may fail.
# Since we are not using this Makefile for compilation, limiting parallelism will not increase build time.
.NOTPARALLEL:

.PHONY: all build install clean unit_test integration_test proto proto_banner reshard_tests e2e_test minimaltools tools generate_ci_workflows

all: build

# Set a custom value for -p, the number of packages to be built/tested in parallel.
# This is currently only used by our Travis CI test configuration.
# (Also keep in mind that this value is independent of GOMAXPROCS.)
ifdef VT_GO_PARALLEL_VALUE
export VT_GO_PARALLEL := -p $(VT_GO_PARALLEL_VALUE)
endif

ifdef VT_EXTRA_BUILD_FLAGS
export EXTRA_BUILD_FLAGS := $(VT_EXTRA_BUILD_FLAGS)
endif

# This should be the root of the vitess Git directory.
ifndef VTROOT
export VTROOT=${PWD}
endif

# This is where Go will install binaries in response to `go build`.
export VTROOTBIN=${VTROOT}/bin

# build the vitess binaries with dynamic dependency on libc
build-dyn:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go build -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		-ldflags "$(shell tools/build_version_flags.sh)"  \
		-o ${VTROOTBIN} ./go/...

# build the vitess binaries statically
build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	# build all the binaries by default with CGO disabled.
	# Binaries will be placed in ${VTROOTBIN}.
	CGO_ENABLED=0 go build \
		    -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		    -ldflags "$(shell tools/build_version_flags.sh)" \
		    -o ${VTROOTBIN} ./go/...

# cross-build can be used to cross-compile Vitess client binaries
# Outside of select client binaries (namely vtctlclient & vtexplain), cross-compiled Vitess Binaries are not recommended for production deployments
# Usage: GOOS=darwin GOARCH=amd64 make cross-build
cross-build:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env

	# For the specified GOOS + GOARCH, build all the binaries by default
	# with CGO disabled. Binaries will be placed in
	# ${VTROOTBIN}/${GOOS}_${GOARG}.
	mkdir -p ${VTROOTBIN}/${GOOS}_${GOARCH}
	CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build         \
		    -trimpath $(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		    -ldflags "$(shell tools/build_version_flags.sh)" \
		    -o ${VTROOTBIN}/${GOOS}_${GOARCH} ./go/...

	@if [ ! -x "${VTROOTBIN}/${GOOS}_${GOARCH}/vttablet" ]; then \
		echo "Missing vttablet at: ${VTROOTBIN}/${GOOS}_${GOARCH}." && exit; \
	fi

debug:
ifndef NOBANNER
	echo $$(date): Building source tree
endif
	bash ./build.env
	go build -trimpath \
		$(EXTRA_BUILD_FLAGS) $(VT_GO_PARALLEL) \
		-ldflags "$(shell tools/build_version_flags.sh)"  \
		-gcflags -'N -l' \
		-o ${VTROOTBIN} ./go/...

# install copies the files needed to run Vitess into the given directory tree.
# This target is optimized for docker images. It only installs the files needed for running vitess in docker
# Usage: make install PREFIX=/path/to/install/root
install: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOTBIN}/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup,vtconsensus} "$${PREFIX}/bin/"

# Will only work inside the docker bootstrap for now
cross-install: cross-build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "${VTROOTBIN}/${GOOS}_${GOARCH}/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup,vtconsensus} "$${PREFIX}/bin/"

# Install local install the binaries needed to run vitess locally
# Usage: make install-local PREFIX=/path/to/install/root
install-local: build
	# binaries
	mkdir -p "$${PREFIX}/bin"
	cp "$${VTROOT}/bin/"{mysqlctl,mysqlctld,vtorc,vtadmin,vtctl,vtctld,vtctlclient,vtctldclient,vtgate,vttablet,vtbackup,vtconsensus} "$${PREFIX}/bin/"

vtctldclient: go/vt/proto/vtctlservice/vtctlservice.pb.go
	make -C go/vt/vtctl/vtctldclient

parser:
	make -C go/vt/sqlparser

codegen: asthelpergen sizegen parser

visitor: asthelpergen
	echo "make visitor has been replaced by make asthelpergen"

asthelpergen:
	go generate ./go/vt/sqlparser/...

sizegen:
	go run ./go/tools/sizegen/sizegen.go \
		--in ./go/... \
		--gen vitess.io/vitess/go/pools.Setting \
		--gen vitess.io/vitess/go/vt/schema.DDLStrategySetting \
		--gen vitess.io/vitess/go/vt/vtgate/engine.Plan \
		--gen vitess.io/vitess/go/vt/vttablet/tabletserver.TabletPlan \
		--gen vitess.io/vitess/go/sqltypes.Result

clean:
	go clean -i ./go/...
	rm -rf third_party/acolyte
	rm -rf go/vt/.proto.tmp

# Remove everything including stuff pulled down by bootstrap.sh
cleanall: clean
	# directories created by bootstrap.sh
	# - exclude vtdataroot and vthook as they may have data we want
	rm -rf bin dist lib pkg
	# Remind people to run bootstrap.sh again
	echo "Please run 'make tools' again to setup your environment"

unit_test: build dependency_check
	echo $$(date): Running unit tests
	tools/unit_test_runner.sh

e2e_test: build
	tools/wesql_cluster_test.sh

e2e_test_scheduler: build
	tools/wesql_onlineddl_scheduler.sh

e2e_test_jobcontroller: build
	tools/wesql_jobcontroller.sh

e2e_test_branch: build
	tools/wesql_branch.sh

e2e_test_vrepl: build
	tools/wesql_onlineddl_vrepl.sh

e2e_test_vrepl_stress: build
	tools/wesql_onlineddl_vrepl_stress.sh

e2e_test_vrepl_stress_suite: build
	tools/wesql_onlineddl_vrepl_stress_suite.sh

e2e_test_vrepl_suite: build
	tools/wesql_onlineddl_vrepl_stress_suite.sh

e2e_test_wasm: build
	tools/wesql_wasm.sh

.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS = -ec

install_protoc-gen-go:
	GOBIN=$(VTROOTBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(shell go list -m -f '{{ .Version }}' google.golang.org/protobuf)
	GOBIN=$(VTROOTBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0 # the GRPC compiler its own pinned version
	GOBIN=$(VTROOTBIN) go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@$(shell go list -m -f '{{ .Version }}' github.com/planetscale/vtprotobuf)

PROTO_SRCS = $(wildcard proto/*.proto)
PROTO_SRC_NAMES = $(basename $(notdir $(PROTO_SRCS)))
PROTO_GO_OUTS = $(foreach name, $(PROTO_SRC_NAMES), go/vt/proto/$(name)/$(name).pb.go)
# This rule rebuilds all the go files from the proto definitions for gRPC.
proto: $(PROTO_GO_OUTS)

ifndef NOBANNER
	echo $$(date): Compiling proto definitions
endif

$(PROTO_GO_OUTS): minimaltools install_protoc-gen-go proto/*.proto
	$(VTROOT)/bin/protoc \
		--go_out=. --plugin protoc-gen-go="${VTROOTBIN}/protoc-gen-go" \
		--go-grpc_out=. --plugin protoc-gen-go-grpc="${VTROOTBIN}/protoc-gen-go-grpc" \
		--go-vtproto_out=. --plugin protoc-gen-go-vtproto="${VTROOTBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+size+pool \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/query.Row \
		--go-vtproto_opt=pool=vitess.io/vitess/go/vt/proto/binlogdata.VStreamRowsResponse \
		-I${PWD}/dist/vt-protoc-21.3/include:proto $(PROTO_SRCS)
	cp -Rf vitess.io/vitess/go/vt/proto/* go/vt/proto
	rm -rf vitess.io/vitess/go/vt/proto/


back_to_dev_mode:
	./tools/back_to_dev_mode.sh

tools:
	echo $$(date): Installing dependencies
	./bootstrap.sh

minimaltools:
	echo $$(date): Installing minimal dependencies
	BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

dependency_check:
	./tools/dependency_check.sh

vtadmin_web_install:
	cd web/vtadmin && npm install

# Generate JavaScript/TypeScript bindings for vtadmin-web from the Vitess .proto files.
# Eventually, we'll want to call this target as part of the standard `make proto` target.
# While vtadmin-web is new and unstable, however, we can keep it out of the critical build path.
vtadmin_web_proto_types: vtadmin_web_install
	./web/vtadmin/bin/generate-proto-types.sh

vtadmin_authz_testgen:
	go generate ./go/vt/vtadmin/
	go fmt ./go/vt/vtadmin/

# Generate github CI actions workflow files for unit tests and cluster endtoend tests based on templates in the test/templates directory
# Needs to be called if the templates change or if a new test "shard" is created. We do not need to rebuild tests if only the test/config.json
# is changed by adding a new test to an existing shard. Any new or modified files need to be committed into git
generate_ci_workflows:
	cd test && go run ci_workflow_gen.go && cd ..
	cd .github/workflows && sh replace_workflows.sh && cd ../..

.PHONY: check-license-header
check-license-header: ## Run license header check.
	@./misc/git/hooks/header-check

.PHONY: fix-license-header
fix-license-header: ## Run license header fix.
	@./misc/git/hooks/header-check fix

define buildx_docker_image
	${info Building ${IMG}}
	# Fix permissions before copying files, to avoid AUFS bug other must have read/access permissions
	chmod -R o=rx *;
	echo "Building docker using amd64/arm64 buildx";
	docker buildx build --squash --platform ${BUILDX_PLATFORMS} -f ${1} \
		-t ${IMG}:${VERSION} ${BUILDX_ARGS} ${2} .;
endef

push-images:
	${call buildx_docker_image,docker/wesqlscale/Dockerfile.release,--push}

# make build-image IMG=apecloud/apecloud-mysql-scale VERSION=local-latest BUILDX_PLATFORMS=linux/arm64
build-image:
	${call buildx_docker_image,docker/wesqlscale/Dockerfile.release,--load}

tools/bin/failpoint-ctl:
	GOBIN=$(shell pwd)/tools/bin go install github.com/pingcap/failpoint/failpoint-ctl@2eaa328

FAILPOINT_ENABLE  := find $$PWD/go -type d | grep -vE "(\.git|tools|vtdataroot)" | xargs tools/bin/failpoint-ctl enable
FAILPOINT_DISABLE := find $$PWD/go -type d | grep -vE "(\.git|tools|vtdataroot)" | xargs tools/bin/failpoint-ctl disable

failpoint-enable: tools/bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: tools/bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)