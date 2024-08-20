---
标题: Kubernetes 入门指南
---

Kubernetes 入门指南
=====================

本文介绍了如何在 Kubernetes 集群中快速启动 WeScale 集群，并概述了一些基本的集群操作，例如修改配置、查看日志、连接到集群等。

WeScale 使用 Kubeblocks 作为 Kubernetes Operator。得益于 Kubeblocks 强大的操作能力，你可以在本地或任何云提供商上部署和使用 WeScale。

# 先决条件
## 使用 Kind 安装 Kubernetes 集群
在开始之前，你需要有一个 Kubernetes 集群。如果你还没有，可以使用 Minikube 或 Kind 创建一个集群。

例如，你可以使用以下命令通过 Kind 创建一个 Kubernetes 集群：
```zsh
brew install kind
kind create cluster
```

## 安装 Kbcli

你可以在笔记本电脑或云端虚拟机上安装 kbcli。kbcli 目前支持 macOS、Windows 和 Linux。请参阅以下链接获取安装说明：
https://kubeblocks.io/docs/release-0.8/user_docs/installation/install-with-kbcli/install-kbcli

对于 MacOS 和 Linux 用户，可以使用以下命令安装 kbcli：
```zsh
curl -fsSL https://kubeblocks.io/installer/install_cli.sh | bash
```

## 安装 Kubeblocks

安装好 kbcli 后，你可以使用以下命令安装 Kubeblocks。更多信息请参阅以下链接：
https://kubeblocks.io/docs/release-0.8/user_docs/installation/install-with-kbcli/install-kubeblocks-with-kbcli
```zsh
kbcli kubeblocks install
```

# 集群管理
## 创建一个 WeScale 集群
你只需一个命令即可创建一个 WeScale 集群。例如，你可以使用以下命令创建一个名为 `vt` 的 WeScale 集群：
```zsh
kbcli cluster create mysql vt --mode raftGroup --availability-policy none --proxy-enabled true 
```

## 列出所有 WeScale 集群
你可以使用以下命令列出所有 WeScale 集群：
```zsh
kbcli cluster list
```

## 删除一个 WeScale 集群
你可以使用以下命令删除一个 WeScale 集群：
```zsh
kbcli cluster delete vt
```

# 连接到 WeScale 集群
## 通过 VTGate 连接到 WeScale 集群
```zsh
kbcli cluster connect vt --component vtgate
```

## 直接连接到 WeScale 管理的 MySQL 服务器
```zsh
kbcli cluster connect vt
```

# 配置管理
## 查看配置
* 查看 VTGate 的配置
```zsh
kbcli cluster describe-config vt --components vtgate --show-detail
```
* 查看 VTTablet 的配置
```zsh
kbcli cluster describe-config vt --components mysql --show-detail --config-specs vttablet-config
```
* 查看 MySQL 的配置
```zsh
kbcli cluster describe-config vt --components mysql --show-detail --config-specs mysql-consensusset-config
```

## 修改配置
* 修改 VTGate 的配置
```zsh
kbcli cluster edit-config vt --components vtgate
```
* 修改 VTTablet 的配置
```zsh
kbcli cluster edit-config vt --components mysql --config-spec=vttablet-config
```
* 修改 MySQL 的配置
```zsh
kbcli cluster edit-config vt --components mysql --config-spec=mysql-consensusset-config
```