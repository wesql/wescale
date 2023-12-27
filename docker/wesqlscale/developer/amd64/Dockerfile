# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).



ARG bootstrap_version=14.1
ARG image="vitess/bootstrap:${bootstrap_version}-common"

FROM "${image}"


RUN apt-get update
RUN apt-get install -y sudo curl vim jq default-mysql-client

# Install dependencies
COPY install_dependencies.sh /vt/dist/install_dependencies.sh
RUN /vt/dist/install_dependencies.sh mysql80

COPY install_local_dependencies.sh /vt/dist/install_local_dependencies.sh
RUN  /vt/dist/install_local_dependencies.sh

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Re-copy sources from working tree.
# COPY --chown=vitess:vitess localdocker /vt/src/vitess.io/vitess

# RUN mkdir /vt/src/mount  && chown vitess:vitess /vt/src/mount

RUN echo "source /vt/src/mount/examples/common/env.sh" >> /etc/bash.bashrc
# Build and install Vitess in a temporary output directory.
USER vitess

WORKDIR /vt/src/vitess.io/vitess

# pre download go package
#RUN go mod download

# mount for github source code
VOLUME /vt/src/mount
# mount for cluster data
VOLUME /vt/vtdataroot


ENV VTROOT /vt/src/mount/wesql-scala
ENV VTDATAROOT /vt/vtdataroot
ENV PATH $VTROOT/wesql-scala/bin:$PATH
ENV PATH="/var/opt/etcd:${PATH}"

ARG GOPROXY=https://goproxy.cn
ARG LD_FLAGS="-s -w"
ENV GONOPROXY=github.com/apecloud
ENV GONOSUMDB=github.com/apecloud
ENV GOPRIVATE=github.com/apecloud
ENV GOPROXY=${GOPROXY}

EXPOSE 15306 17100 17101 17102 2379

WORKDIR /vt/src/mount

CMD ["/bin/bash"]

# CMD cd /vt/src/mount/examples/mysql/ && ./init_cluster.sh && /bin/bash
