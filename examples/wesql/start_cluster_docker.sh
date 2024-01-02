# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).



existing_image=$(docker images -q wescala-dev)

# Build the 'wescala-dev' image if it doesn't exist
if [ -z "$existing_image" ]; then
    echo "Building the 'wescala-dev' image..."
    DOCKER_BUILDKIT=0 docker build  ../../docker/wesqlscale/local/ -t wescala-dev
else
    echo "The 'wescala-dev' image already exists."
fi
container_mysql_port=(17100 17101 17102)
vtgate_port=15306
mysql_port=17100,17101,17102
vtctld_port=15999
etcd_port=12379
container_name=""
port_mapping=""
# Parse command line arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --vtgate-port) vtgate_port="$2"; shift;;
    --vttablet-port) vttablet_port="$2"; shift;;
    --vtctld-port) vtctld_port="$2"; shift;;
    --mysql-port) mysql_port="$2"; shift;;
    --etcd-port) etcd_port="$2"; shift;;
    --container-name) container_name="$2"; shift;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
  shift
done
port_mapping="-p $vtgate_port:15306 -p $vtctld_port:15999 -p $etcd_port:2379 "
# Parse command line arguments
ports=($(echo $mysql_port | tr "," "\n"))
index=0
for port in "${ports[@]}"; do
  port_mapping+="-p ${port}:${container_mysql_port[$index]} "
  index=$((index+1))
done
container_name_cmd=""
random_number=$RANDOM
echo random_number: $random_number
if [ -n "$container_name" ]; then
  container_name_cmd="-e container_name=$container_name"
else
  container_name="wescala-dev-$random_number"
  container_name_cmd="--name wescala-dev-$random_number"
fi
# Run the container with the specified settings
docker run -id ${port_mapping} -v $PWD/../../:/vt/src/mount/wesql-scala -v /tmp:/wesqlscale/vt/vtdataroot -e tablet_hostname='0.0.0.0' -e ETCD_SERVER="0.0.0.0:2379" -e GO_FAILPOINTS=$GO_FAILPOINTS ${container_name_cmd} wescala-dev

# 初始化集群
docker exec ${container_name} bash -c "cd /vt/src/mount/wesql-scala/examples/wesql && ./init_cluster_docker.sh"
if docker ps -a | grep -q "${container_name}"; then
    echo "${container_name} init successfully"
else
    echo "${container_name} does not exist"
fi
