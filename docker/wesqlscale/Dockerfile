# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

ARG DEBIAN_VER=bullseye

FROM --platform=linux/amd64 golang:1.20.2-${DEBIAN_VER}

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Allows docker builds to set the BUILD_NUMBER
ARG BUILD_NUMBER

# Allows docker builds to set the BUILD_GIT_BRANCH
ARG BUILD_GIT_BRANCH

# Allows docker builds to set the BUILD_GIT_REV
ARG BUILD_GIT_REV

RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# Install Vitess build dependencies
RUN apt-get update && \
    apt-get upgrade -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
    # TODO(mberlin): Group these to make it easier to understand which library actually requires them.
    curl \
    etcd \
    g++ \
    git \
    make \
    software-properties-common \
    unzip \
    zip \
    xvfb \
    ca-certificates \
    wget \
    default-mysql-client \
    jq \
    curl \
    ssh \
    delve \
    sudo \
    procps \
    rsyslog \
    vim \
    iputils-ping &&\
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG GOPROXY=https://goproxy.cn
ARG LD_FLAGS="-s -w"
ENV GONOPROXY=github.com/apecloud
ENV GONOSUMDB=github.com/apecloud
ENV GOPRIVATE=github.com/apecloud
ENV GOPROXY=${GOPROXY}

# Set up Vitess environment (equivalent to '. dev.env')
ENV VTSOURCEROOT /vt/src/vitess
ENV VTDATAROOT /vtdataroot
ENV VTPORTSTART 15000
ENV PATH $VTSOURCEROOT/bin:$VTSOURCEROOT/dist/maven/bin:$VTSOURCEROOT/dist/chromedriver:$PATH
ENV USER vitess
ENV MYSQL_FLAVOR MySQL80
ENV BUILD_CHROME 0
ENV BUILD_JAVA 0
ENV BUILD_CONSUL 0

# copy sources from working tree
COPY . $VTSOURCEROOT

RUN git config --global --add safe.directory $VTSOURCEROOT

# Create vitess user and Prepare directory structure.
RUN groupadd -r vitess && useradd -r -g vitess vitess && \
    chpasswd && adduser vitess sudo && \
    mkdir -p /vtdataroot /home/vitess && \
    mkdir -p /vt && \
    mkdir -p /vt/bin && \
    mkdir -p /vt/config && \
    mkdir -p /vt/web && \
    chown -R vitess:vitess /vtdataroot /vt /home/vitess


# Bootstrap Vitess
WORKDIR /vt/src/vitess

#USER vitess

# Download vendored Go dependencies
RUN su vitess -c "go mod download"

RUN BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 BUILD_K3S=0 BUILD_ETCD=0 ./bootstrap.sh

# Build Vitess
USER root

RUN su vitess -c "mkdir -p /vt/src/vitess.io/vitess/bin"
ENV VTROOT /vt/src/vitess

# Binaries will be placed in ${VTROOT}/bin.
RUN VT_GO_PARALLEL_VALUE=8 make build

RUN go clean -modcache && \
    go clean -cache

ENV VTROOT /vt

# Copy binaries
RUN cp /vt/src/vitess/bin/vtctld /vt/bin/ && \
    cp /vt/src/vitess/bin/vtctl /vt/bin/ && \
    cp /vt/src/vitess/bin/vtctlclient /vt/bin/ && \
    cp /vt/src/vitess/bin/vtctldclient /vt/bin/ && \
    cp /vt/src/vitess/bin/vtgate /vt/bin/ && \
    cp /vt/src/vitess/bin/vtconsensus /vt/bin/ && \
    cp /vt/src/vitess/bin/vttablet /vt/bin/ && \
    cp /vt/src/vitess/bin/vtadmin /vt/bin/ && \
    cp /vt/src/vitess/bin/protoc  /vt/bin && \
    cp -rf $VTSOURCEROOT/web /vt && \
    rm -rf /vt/src

WORKDIR /vt

ENV PATH $VTROOT/bin:$PATH

CMD ["/bin/bash"]