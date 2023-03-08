#!/bin/bash
set -ex

vt_base_version='latest'
debian_versions='bullseye'
default_debian_version='bullseye'

if [[ "$(docker images -q apecloud/vitessbase:$vt_base_version 2> /dev/null)" == "" ]]; then
  docker pull --platform linux/amd64 apecloud/vitessbase:$vt_base_version
fi

for debian_version in $debian_versions
do
  echo "####### Building vitess/vt:$debian_version"

  docker build  --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vitesskubeblocks:$vt_base_version-$debian_version kubeblocks
  docker tag apecloud/vitesskubeblocks:$vt_base_version-$debian_version registry.cn-hangzhou.aliyuncs.com/apecloud/vitesskubeblocks:$vt_base_version
  if [[ $debian_version == $default_debian_version ]]; then docker push  registry.cn-hangzhou.aliyuncs.com/apecloud/vitesskubeblocks:$vt_base_version; fi
done
