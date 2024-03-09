# docker network create wescale_clusters
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).




cd ../../

PREFIX=./bin/tmp GOOS=linux GOARCH=amd64 make cross-install

cd examples/wesql

# product
./start_cluster_docker.sh --etcd-port 12379 --mysql-port 18100,18101,18102 --vttablet-port 19100,19101,19102 --grpc-port 20100,20101,20102 --vtgate-port 15307 --vtctld-port 16999 --container-name "product_cluster"

# dev
./start_cluster_docker.sh --etcd-port 12479 --mysql-port 28100,28101,28102 --vttablet-port 29100,29101,29102 --grpc-port 22100,22101,22102 --vtgate-port 15308 --vtctld-port 17999 --container-name "dev_cluster"

echo "prepare data"
cd ../workflow/Branch && ./prepare.sh
echo "prepare data done"

vtctlclient Mount --server localhost:17999  --  --type vitess  --topo_type etcd2 --topo_server 172.19.0.2:12379 --topo_root /vitess/global release_cluster

vtctlclient --server localhost:17999 Branch --  --external_cluster release_cluster --source_database branch_source --target_database branch_target --skip_copy_phase=false --workflow_name branch_test --default_filter_rules "RAND()<0.1" Prepare