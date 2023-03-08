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

ARG VT_BASE_VER=latest
ARG DEBIAN_VER=stable-slim

FROM registry.cn-hangzhou.aliyuncs.com/apecloud/vitessbase:${VT_BASE_VER} AS kubeblocks

FROM debian:${DEBIAN_VER}

RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# TODO: remove when https://github.com/vitessio/vitess/issues/3553 is fixed
RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install wget default-mysql-client jq curl ssh telnet delve -qq --no-install-recommends sudo etcd procps rsyslog vim iputils-ping && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTSOURCEROOT /vt/src/vitess.io/vitess
ENV VTROOT /vt
ENV VTDATAROOT /vtdataroot

# Prepare directory structure.
RUN mkdir -p /vt && \
    mkdir -p /vt/bin && \
    mkdir -p /vt/config && \
    mkdir -p /vt/web && \
    mkdir -p /vt/scripts && \
    mkdir -p /vtdataroot

RUN cd /vt  \
    && wget --no-check-certificate https://golang.google.cn/dl/go1.19.7.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf /vt/go1.19.7.linux-amd64.tar.gz

ENV PATH /usr/local/go/bin:$PATH
ENV GOROOT /usr/local/go/


# Copy CA certs for https calls
COPY --from=kubeblocks /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=kubeblocks $VTSOURCEROOT/config/apecloud_scripts /vt/scripts

# Copy binaries
COPY --from=kubeblocks /vt/bin/mysqlctld /vt/bin/
COPY --from=kubeblocks /vt/bin/mysqlctl /vt/bin/
COPY --from=kubeblocks /vt/bin/vtctld /vt/bin/
COPY --from=kubeblocks /vt/bin/vtctl /vt/bin/
COPY --from=kubeblocks /vt/bin/vtctlclient /vt/bin/
COPY --from=kubeblocks /vt/bin/vtctldclient /vt/bin/
COPY --from=kubeblocks /vt/bin/vtgate /vt/bin/
COPY --from=kubeblocks /vt/bin/vtconsensus /vt/bin/
COPY --from=kubeblocks /vt/bin/vttablet /vt/bin/
COPY --from=kubeblocks /vt/bin/vtbackup /vt/bin/
COPY --from=kubeblocks /vt/bin/vtadmin /vt/bin/

# copy web admin files
COPY --from=kubeblocks $VTSOURCEROOT/web /vt/web/

# add vitess user/group and add permissions
RUN groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chpasswd && adduser vitess sudo && \
   chown -R vitess:vitess /vt && \
   chown -R vitess:vitess /vtdataroot

ENV PATH $VTROOT/bin:$PATH

CMD ["/bin/bash"]