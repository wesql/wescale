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

# Download vendored Go dependencies
RUN cd /vt/src/vitess && \
 su vitess -c "go mod download -x"

# Bootstrap Vitess
WORKDIR /vt/src/vitess

USER vitess

RUN BUILD_CHROME=0 BUILD_JAVA=0 BUILD_CONSUL=0 ./bootstrap.sh

# Build Vitess
USER root

RUN su vitess -c "mkdir -p /vt/src/vitess.io/vitess/bin"
ENV VTROOT /vt/src/vitess

# Binaries will be placed in ${VTROOT}/bin.
RUN VT_GO_PARALLEL_VALUE=8 make build

RUN go clean -modcache
RUN go clean -cache

ENV VTROOT /vt

# Copy binaries
RUN cp /vt/src/vitess/bin/vtctld /vt/bin/
RUN cp /vt/src/vitess/bin/vtctl /vt/bin/
RUN cp /vt/src/vitess/bin/vtctlclient /vt/bin/
RUN cp /vt/src/vitess/bin/vtctldclient /vt/bin/
RUN cp /vt/src/vitess/bin/vtgate /vt/bin/
RUN cp /vt/src/vitess/bin/vtconsensus /vt/bin/
RUN cp /vt/src/vitess/bin/vttablet /vt/bin/
RUN cp /vt/src/vitess/bin/vtbackup /vt/bin/
RUN cp /vt/src/vitess/bin/vtadmin /vt/bin/
RUN cp /vt/src/vitess/bin/etcd  /vt/bin
RUN cp /vt/src/vitess/bin/etcdctl /vt/bin
RUN cp /vt/src/vitess/bin/k3s  /vt/bin
RUN cp /vt/src/vitess/bin/protoc  /vt/bin

# copy web admin files
RUN cp -rf $VTSOURCEROOT/web /vt

WORKDIR /vt

RUN rm -rf /vt/src

ENV PATH $VTROOT/bin:$PATH

CMD ["/bin/bash"]