# after running create_branch_cluster.sh,
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).

# you can run this script to test branch cluster
echo "alter table schema on feature1_db, add a new column feature1_col
"
mysql -h127.0.0.1 -P15308 -e "alter table feature1_db.corder add column feature1_col int;"

# wait for online-ddl complete
sleep 3

echo "alter table schema on feature2_db, add a new column feature2_col
"
mysql -h127.0.0.1 -P15308 -e "alter table feature2_db.corder add column feature2_col int;"
# wait for online-ddl complete
sleep 3

echo "prepare merge back for feature1_db, there should be no conflict
"
vtctlclient --server localhost:17999 Branch --  --workflow_name dev_feature1 --merge_option diff prepareMergeBack

echo "start merge back for feature1_db, should succeed
"
vtctlclient --server localhost:17999 Branch --  --workflow_name dev_feature1 startMergeBack
# wait for online-ddl complete
sleep 3

echo "prepare merge back for feature2_db, should conflict, because dev_db.corder has add feature1_col
"
vtctlclient --server localhost:17999 Branch --  --workflow_name dev_feature2 --merge_option diff prepareMergeBack

echo "alter table schema on feature2_db, drop column feature2_col, and create a new table feature2_table
"
mysql -h127.0.0.1 -P15308 -e "alter table feature2_db.corder drop column feature2_col;"
mysql -h127.0.0.1 -P15308 -e "create table feature2_db.feature2_table (id int, primary key(id));"
# wait for online-ddl complete
sleep 10

echo "prepare merge back for feature2_db, there should be no conflict this time
"
vtctlclient --server localhost:17999 Branch --  --workflow_name dev_feature2 --merge_option diff prepareMergeBack

echo "start merge back for feature2_db, should succeed
"
vtctlclient --server localhost:17999 Branch --  --workflow_name dev_feature2 startMergeBack
# wait for online-ddl complete
sleep 3

echo "prepare merge back for dev_db, should override schemas of master
"
vtctlclient --server localhost:17999 Branch --  --workflow_name master_dev --merge_option override prepareMergeBack

echo "start merge back for dev_db, should succeed
"
vtctlclient --server localhost:17999 Branch --  --workflow_name master_dev startMergeBack
# wait for online-ddl complete
sleep 15

echo "prepare merge back for master_db, should override schemas of release
"
vtctlclient --server localhost:17999 Branch --  --merge_option override --workflow_name release_master prepareMergeBack

echo "start merge back for dev_db, should succeed
"
vtctlclient --server localhost:17999 Branch --  --workflow_name release_master startMergeBack
# wait for online-ddl complete
sleep 15

