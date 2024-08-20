---
标题: 认证和授权
---

# 背景

这篇文章讨论了两个问题：用户认证和授权管理。

**用户认证**：这个部分解决的是登录问题，确保用户可以以特定的“身份”登录到数据库。

**授权管理**：这个部分处理的是权限问题，确保用户在当前的“身份”下，能够对资源执行读/写/管理操作。

# 用户认证

WeSQL WeScale 提供了三种用户管理机制：`none`（无认证）、`static`（静态文件认证）和`mysqlbased`（基于 MySQL 的认证）。

## none（无认证）
跳过用户认证，因此用户不需要提供用户名或密码就可以登录到 VTGate。这在安全环境中有利于性能，因为它避免了认证过程带来的开销。

**使用方法**：

VTGate 启动参数：
+ `--mysql_auth_server_impl=none`

## static（静态文件认证）
用户名和密码存储在一个静态文件中。代理通过这个文件验证用户凭证。使用 static 的优势在于代理管理用户认证数据。如果代理的后端连接了多个 MySQL 集群，一个静态文件就可以解决认证问题。（但目前代理只支持连接一个 MySQL 集群，所以相较于“mysqlbased”没有优势。）

**使用方法**：

VTGate 启动参数：
+ `--mysql_auth_server_impl=static`
+ `--mysql_auth_server_static_file`：用户信息文件的路径。注意，用户信息文件应该是 JSON 格式，结构如下：

```shell
$ cat > users.json << EOF
{
  "vitess": [
    {
      "UserData": "vitess",
      "Password": "supersecretpassword"
    }
  ],
  "myuser1": [
    {
      "UserData": "myuser1",
      "Password": "password1"
    }
  ],
  "myuser2": [
    {
      "UserData": "myuser2",
      "Password": "password2"
    }
  ]
}
EOF
```

## mysqlbased（基于 MySQL 的认证）
代理通过 mysql.user 表来验证用户凭证。

它支持 `mysql_native_password` 和 `caching_sha2_password` 密码插件。当使用 `caching_sha2_password` 时，用户必须通过 UnixSocket 或 TLS 安全连接来连接代理。原理是：代理与 MySQL 的认证过程（传输协议 + 算法）兼容。代理会定期从 `mysql.user` 表加载数据到 VTGate 的内存中。当用户登录到 VTGate 时，会根据这些数据进行验证。在整个过程中，代理“像 MySQL 一样”运作，因此它兼容大多数 MySQL 客户端。

在 `mysqlbased` 模式下，用户信息来自后端的 MySQL 主节点。用户需要连接到这个主节点来创建用户账户。在 VTGate 更新相应的用户信息后，用户就可以登录 VTGate 了。

**使用方法**：

VTGate 启动参数：

+ `--mysql_auth_server_impl=mysqlbased`：设置验证模式为 `mysqlbased`。
+ `--mysql_server_require_secure_transport`：如果使用 `caching_sha2_password`，则设置此参数。
+ `--mysql_server_ssl_key`：配置 SSL 密钥。如果使用 `caching_sha2_password`，则设置此参数。
+ `--mysql_server_ssl_cert`：配置 SSL 证书。如果使用 `caching_sha2_password`，则设置此参数。

## reload 命令使用方法
VTGate 支持 `reload users` 和 `reload privileges` 命令。在主 MySQL 实例上创建用户或修改用户权限后，如果希望立即生效，可以在 VTGate 中执行 `reload` 命令，如下所示：
```
mysql> reload users;
Query OK, 11 rows affected (0.00 sec)
mysql> reload privileges;
Query OK, 0 rows affected (0.00 sec)
```
注意：`reload privileges` 不会显示受影响的行数。因此，`Query OK` 消息表示命令成功执行。

# 授权管理
目前，WeSQL WeScale 在内部将用户分为三种类型：

**Reader**：与读 DML（例如 SELECT）相关。

**Writer**：与写 DML（例如 INSERT, UPDATE, DELETE）相关。

**Admin**：与 DDL（例如 ALTER TABLE）相关。

WeSQL WeScale 的授权是在 vt tablet 层进行的，支持 `simple` 和 `mysqlbased` 两种方法。

## simple（简单认证）
建议与“static”认证方法一起使用。这是 Vitess 的原生功能。用户权限数据存储在一个静态文件中，代理通过这个文件验证用户权限。

**使用方法**：

VTTablet 启动参数：

+ `--queryserver_config_strict_table_acl`：如果设置为 true，则启用权限检查。
+ `--table_acl_config_mode=simple`：指定认证模式为 simple。该参数还支持 `mysqlbased`。
+ `--table_acl_config`：指定静态文件的路径，文件应为 JSON 格式，结构如下：

```json
{
  "table_groups": [
    {
      "name": "aclname",
      "table_names_or_prefixes": ["%" ],
      "readers": ["vtgate-user1" ],
      "writers": ["vtgate-user2"],
      "admins": ["vtgate-user3"]
    }
  ]
}
```
- `--enforce_tableacl_config`：如果启用，它会强制检查 `table_acl_config` 文件中 JSON 的格式。如果解析失败，它会终止 vttablet 程序。
- `--table_acl_config_reload_interval=30s`：重新加载静态文件的时间间隔，默认为 30 秒。

## mysqlbased（基于 MySQL 的认证）

> 建议与“mysqlbased”认证方法一起使用。

代理通过 MySQL 的权限信息来检查用户权限。

在 `mysqlbased` 模式下，用户通过连接到 MySQL 后端来控制权限。等待vttablet同步权限信息后，就可以应用权限。在 vttablet 中，角色被抽象为 reader, writer 和 admin。如果只授予了部分写权限（例如仅 insert），则不会被视为 writer。

**Reader**：拥有 select 权限的用户被视为 reader。

**Writer**：拥有 insert, update 和 delete 权限的用户被视为 writer。

**Admin**：对于全局（`.*`）权限，只有超级用户被视为 admin。对于数据库级（`{database.*}`）和表级（`{database}.{table}`）权限，需要拥有“所有权限”才能被视为 admin。

授予用户 admin 角色的示例：
```sql
GRANT SUPER ON *.* TO 'user'@'localhost' # 全局级别
GRANT ALL PRIVILEGES ON test.* TO 'user'@'localhost' # 数据库级别
GRANT ALL PRIVILEGES ON test.table TO 'user'@'localhost' # 表级别
```

**使用方法**：

VTTablet 启动参数

+ `--queryserver_config_strict_table_acl`：如果设置为 true，则启用权限检查。
+ `--table_acl_config_reload_interval=30s`：从 MySQL 表重新加载用户信息的时间间隔。
+ `--table_acl_config_mode=mysqlbased`：指定认证模式为 `mysqlbased`。