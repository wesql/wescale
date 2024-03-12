# docker network create wescale_clusters
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
cd ../../

PREFIX=./bin/tmp GOOS=linux GOARCH=amd64 make cross-install

cd examples/wesql

# create docker network wescale_clusters if it not exists
if ! docker network inspect wescale_clusters &>/dev/null; then
    docker network create wescale_clusters
    echo "create docker network wescale_clusters."
else
    echo "docker network wescale_clusters already exists."
fi

# product
echo "create release cluster"
./start_cluster_docker.sh --etcd-port 12379 --mysql-port 18100,18101,18102 --vttablet-port 19100,19101,19102 --grpc-port 20100,20101,20102 --vtgate-port 15307 --vtctld-port 16999 --container-name "release_cluster" --network-name wescale_clusters

echo "create dev cluster"
# dev
./start_cluster_docker.sh --etcd-port 12479 --mysql-port 28100,28101,28102 --vttablet-port 29100,29101,29102 --grpc-port 22100,22101,22102 --vtgate-port 15308 --vtctld-port 17999 --container-name "dev_cluster" --network-name wescale_clusters

echo "prepare data"
cd ../workflow/Branch && ./prepare.sh
echo "prepare data done"

echo "mount release cluster on dev cluster
"
vtctlclient Mount --server localhost:17999  --  --type vitess  --topo_type etcd2 --topo_server 172.20.0.2:12379 --topo_root /vitess/global release_cluster

echo "prepare branch
"
vtctlclient --server localhost:17999 Branch --  --external_cluster release_cluster --source_database release_db --target_database master_db --skip_copy_phase=false --stop_after_copy=false --workflow_name release_master Prepare

echo "prepare branch start
"
vtctlclient --server localhost:17999 Branch --  --workflow_name release_master Start


echo "alter table schema on dev cluster
"
mysql -h127.0.0.1 -P15308 -e "alter table master_db.corder add column col1 int;"

sleep 3

echo "prepare merge back
"
vtctlclient --server localhost:17999 Branch --  --workflow_name release_master prepareMergeBack

echo "start merge back
"
vtctlclient --server localhost:17999 Branch --  --workflow_name release_master startMergeBack