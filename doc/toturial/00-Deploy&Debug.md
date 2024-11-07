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

## Debug

If you would like to debug `VTGate` or `VTTablet`, you may want to start them in IDE.

You can kill the vtgate/vttablet process started by the script and start vtgate/vttablet from an IDE for debugging. The program arguments can refer to the startup options of vtgate got from `ps aux` command:

![img](images/ps_aux.png)

There is a convenient way to help you start a local cluster for debugging purposes. You can add an environment variable `debug=on` when starting a cluster.

It will start a cluster first, then all vtgate and vttablet processes will be killed.

```Shell
debug=on ./init_cluster.sh
```

Let's take VTTablet as an example:

You should configure the `Run kind`、`Package path`、`Working directory`、`Environment` and `Program arguments`。

You can follow the settings in the picture, but remember to replace the file paths with the ones on your own computer. As stated previously, program arguments can be viewed by using the `ps aux` command.

![img](images/debug_goland.png)

However, when using the `ps aux` command to obtain information about the program arguments of `vttablet`, there is no argument value following the `--tablet_hostname` flag, which will lead an error in GoLand. You can obtain the hostname by running `hostname` in the terminal and then use it as the value of `hostname` flag.

