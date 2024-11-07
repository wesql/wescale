---
title: Start to deploy & debug a local cluster
---

Start to deploy & debug a local cluster
=====================

This document explains how to start a WeSQL WeScale cluster on your local machine and begin debugging. We have made every effort to make this document intuitive and helpful for a successful deployment, but inevitably, you may encounter some corner cases that lead to failure. If you have any issues, please feel free to open an issue. We also welcome your help in improving the document if you are willing to do so.

# **Pre-requisites**

Start a WeSQL WeScale cluster locally, which relies on the following software.

* [Docker](https://docs.docker.com/engine/install/)
* [Go 1.20](https://go.dev/dl/#go1.20.1)
* [Make](https://www.gnu.org/software/make/)

You can verify the installation of these dependencies by running the following commands:

```Shell
docker version
go version
make --version
```

# **Build & Install**

## **Pull Source Code**

```Shell
git clone https://github.com/wesql/wescale.git
cd wescale
```

## **Build**

You can call `make build` command to build the whole project. If the compilation is successful, you can use `ls bin` to see that many binary executable files have been generated.

```Shell
BUILD_ETCD=1 ./bootstrap.sh
make build
```

## Install

It will take some time for WeSQL WeScale to build. Once it completes, you should see a bin folder which will hold the  WeSQL WeScale binaries. You will need to add this folder to your `PATH` variable, so that the command line can directly call the newly generated binary file.

```Shell
ls bin
export PATH=$PATH:$(pwd)/bin
```

# **Deploy** **&** **Debug**

You can deploy a WeSQL WeScale cluster locally by using the scripts in `wescale/examples/wesql-server` folder

## **Deploy**

```Shell
cd ./examples/wesql-server

# Start the MySQL server
./start_mysql_server.sh

# Start the WeSQL WeScale cluster
./init_single_node_cluster.sh
```

Once the WeSQL WeScale cluster is successfully launched, you can use the following command to connect to WeSQL WeScale

```Shell
mysql -h127.0.0.1 -P15306
```