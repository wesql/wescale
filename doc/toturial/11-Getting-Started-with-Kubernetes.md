---
title: Getting Started with Kubernetes
---

Getting Started with Kubernetes
=====================

This article describes how to quickly launch a WeScale cluster in a Kubernetes cluster and outlines some basic cluster operations, such as modifying configurations, viewing logs, connecting to the cluster, and more.

WeScale uses Kubeblocks as the Kubernetes Operator. Thanks to the powerful operational capabilities of Kubeblocks, you can deploy and use WeScale locally or with any cloud provider.

# Prerequisites
## Install Kubernetes Cluster With Kind
Before you begin, you need to have a Kubernetes cluster. If you don't have one, you can create a cluster using Minikube or Kind.

For example, you can use Kind to create a Kubernetes cluster with the following commands:
```zsh
brew install kind
kind create cluster
```

## Install Kbcli

You can install kbcli on your laptop or virtual machines on the cloud. kbcli now supports macOS, Windows, and Linux.
Please refer to the following link for installation instructions:
https://kubeblocks.io/docs/release-0.8/user_docs/installation/install-with-kbcli/install-kbcli

For MacOS & Linux users, you can install kbcli with the following command:
```zsh
curl -fsSL https://kubeblocks.io/installer/install_cli.sh | bash
```

## Install Kubeblocks

Once you have installed kbcli, you can install Kubeblocks with the following command. For more information, please refer to the following link:
https://kubeblocks.io/docs/release-0.8/user_docs/installation/install-with-kbcli/install-kubeblocks-with-kbcli
```zsh
kbcli kubeblocks install
```

# Cluster Management
## Create a WeScale Cluster
You can create a WeScale cluster with just one command. For example, you can create a WeScale cluster named `vt` with the following command:
```zsh
kbcli cluster create mysql vt --mode raftGroup --availability-policy none --proxy-enabled true 
```

## List All WeScale Clusters
You can list all WeScale clusters with the following command:
```zsh
kbcli cluster list
```

## Delete a WeScale Cluster
You can delete a WeScale cluster with the following command:
```zsh
kbcli cluster delete vt
```

# Connect to a WeScale Cluster
## Connect to a WeScale Cluster Through VTGate
```zsh
kbcli cluster connect vt --component vtgate
```

## Connect Directly to the MySQL Server Managed by WeScale
```zsh
kbcli cluster connect vt
```

# Configurations
## View the Configuration
* View the Configuration of VTGate
```zsh
kbcli cluster describe-config vt --components vtgate --show-detail
```
* View the Configuration of VTTablet
```zsh
kbcli cluster describe-config vt --components mysql --show-detail --config-specs vttablet-config
```
* View the Configuration of MySQL
```zsh
kbcli cluster describe-config vt --components mysql --show-detail --config-specs mysql-consensusset-config
```


## Modify the Configuration
* Modify the Configuration of VTGate
```zsh
kbcli cluster edit-config vt --components vtgate
```
* Modify the Configuration of VTTablet
```zsh
kbcli cluster edit-config vt --components mysql --config-spec=vttablet-config
```
* Modify the Configuration of MySQL
```zsh
kbcli cluster edit-config vt --components mysql --config-spec=mysql-consensusset-config
```




