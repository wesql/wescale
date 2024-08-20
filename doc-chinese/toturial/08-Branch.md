---
题目: Branch
---

# **背景**

branch功能用于将数据库复制到另一个 WeScale 集群或本地集群。在生产环境中，您可以使用branch功能创建与开发环境中相同的数据库。本文档将解释如何使用branch功能。`Branch` 是一个“伞式”命令，其中的 `action` 子命令定义了工作流上的操作。每个 `action` 都可以有自己的 `options`。

# **branch命令的一般语法**

branch操作由 vtctld 服务器执行，该服务器维护与 vttablets 的 gRPC 连接。本质上，vtctld 扮演指挥者的角色。

注意：如果要使用branch功能，**请确保写入端的 vtctld 服务器已打开。**
```
Branch -- <options> <action>

action := [Prepare | Start | Stop | PrepareMergeBack | StartMergeBack | Cleanup ｜ Schemadiff]
-- 每个 action 都有自己的选项，可以用来控制其行为。
```

# **Actions**

branch命令包含 Prepare、Start、Stop、PrepareMergeBack、StartMergeBack 和 SchemaDiff 命令。

如果您的目标只是复制一个数据库，请专注于 'Prepare'、'Start' 和 'Stop' 命令。这些命令可用于完成数据库复制工作流。

'PrepareMergeBack' 和 'StartMergeBack' 命令通常用于将目标数据库的Schema与源数据库合并。

在讲解branch功能的教程之前，我们将使用以下环境作为教程的主要示例。执行以下 SQL 命令创建数据库和表：
```sql
mysql -h127.0.0.1 -P15306 -e 'create database if not exists branch_source'

mysql -h127.0.0.1 -P15306 -e 'create table if not exists branch_source.product(
                                sku varchar(128),
                                description varchar(128),
                                price bigint,
                                primary key(sku)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.customer(
                                customer_id bigint not null auto_increment,
                                email varchar(128),
                                primary key(customer_id)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.corder(
                                order_id bigint not null auto_increment,
                                customer_id bigint,
                                sku varchar(128),
                                price bigint,
                                primary key(order_id)
                              ) ENGINE=InnoDB;
                              CREATE TABLE if not exists branch_source.user (
                                  id INT AUTO_INCREMENT PRIMARY KEY auto_increment,
                                  name VARCHAR(255) NOT NULL
                              ) ENGINE=InnoDB;'
```

## **Prepare**

在Prepare阶段，vtctld 将任务和相应的 table_rules 插入到 mysql.branch_jobs 和 mysql.branch_table_rules 表中。您可以通过修改 mysql.branch_table_rules 中的信息来自定义过滤器。
```
Branch -- 
--source_database=<source_database> 
--target_database=<target_database> 
--workflow_name=<workflow_name> 
[--source_topo_url=<source_topo_url>] 
[--tablet_types=<source_typelet_type>] 
[--cells=<cells>] 
[--include=<tables>]
[--exclude=<tables>]
[--skip_copy_phase =<true/false>]
[--stop_after_copy=<true/false>]
[--default_filter_rules=<filter_rule>]
Prepare
```
+ source_database : 指定源数据库名称。
+ target_database : 指定目标数据库名称（如果不存在，将自动创建）。
+ workflow_name   : 指定branch工作流名称（用于查询branch状态）。
+ source_topo_url : 源集群的拓扑服务器 URL（默认为本地拓扑服务器）。
+ tablet_types    : 数据源 tablet 类型（如 REPLICA、PRIMARY 等）。
+ include         : 指定要从源数据库中包含的表。
+ exclude         : 指定要从源数据库中排除的表。
+ skip_copy_phase : 仅复制模式，不复制数据。
+ stop_after_copy : 如果为 false，则在复制完成后持续同步源和目标之间的数据。
+ default_filter_rules : 在 'where' 子句中附加条件以过滤特定数据。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --source_database branch_source --target_database branch_target --skip_copy_phase=false  --stop_after_copy=false --workflow_name
 branch_test --default_filter_rules "RAND()<0.1" Prepare

successfully create branch workflow : branch_test sourceDatabase : branch_source targetDatabase : branch_target
rules : 
[source_table:"corder" target_table:"corder" filtering_rule:"select * from corder WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"customer" target_table:"customer" filtering_rule:"select * from customer WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"product" target_table:"product" filtering_rule:"select * from product WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
[source_table:"user" target_table:"user" filtering_rule:"select * from `user` WHERE RAND()<0.1" create_ddl:"copy" merge_ddl:"copy" default_filter_rules:"RAND()<0.1"]
```

## **data transformer**

在数据流开始之前，您可以通过修改 mysql.branch_table_rules 表中的 filtering_rule 属性来执行数据转换。我们兼容 [gofakeit](https://github.com/brianvoe/gofakeit) 中的内容。您可以使用 gofakeit_generate 生成特定字符串或使用 gofakeit_bytype 生成特定类型。

过滤规则将在源 MySQL 实例上执行，并在 vttablet 上进行转换和过滤。

### **用法**
```sql
update mysql.branch_table_rules set filtering_rule='select id, gofakeit_generate(\'{firstname}:###:???:{moviename}\') as name from user WHERE id<=100' where source_table_name = 'user';
update mysql.branch_table_rules set filtering_rule='select customer_id, gofakeit_bytype(\'regex\',\'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$\') as email from `customer` WHERE customer_id<=100' where source_table_name = 'customer';
update mysql.branch_table_rules set filtering_rule='select sku,description,gofakeit_bytype(\'intrange\',110,150) as price,gofakeit_bytype(\'floatrange\',23.5,23.9) as weight from `product`' where source_table_name = 'product';
update mysql.branch_table_rules set filtering_rule='SELECT order_id,gofakeit_bytype(\'bigint\') as customer_id,gofakeit_generate(\'{firstname}:###:???:{moviename}\') as sku,gofakeit_bytype(\'bigint\') as price FROM corder where customer_id<=100' where source_table_name = 'corder';
```

## **Start**

`Start` 将在源端和目标端之间启动数据流。目标端将根据 table_rules 执行 ETL 操作。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Start

Start workflow:branch_test successfully.
```

## **Stop**

`Stop` 将停止先前的数据流，您可以使用 `Start` 命令再次启动数据流。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test Stop

Start workflow branch_test successfully
```

注意：如果branch复制阶段已经完成且 stop_after_copy 设置为 true，则 Stop 功能将失效。

## **SchemaDiff**

`SchemaDiff` 显示两个数据库模式之间的差异。该工具有助于决定是否将更改合并回源数据库。

### **用法**
```shell
mysql -h127.0.0.1 -P15306 -e 'alter table branch_target.product add column v2 int;'
mysql -h127.0.0.1 -P15306 -e 'alter table branch_target.product add column v3 int;'

vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test schemadiff

table product is diff
branch_target:name:"product" schema:"CREATE TABLE `product` (\n  `sku` varchar(128) NOT NULL,\n  `description` varchar(128) DEFAULT NULL,\n  `price` bigint DEFAULT NULL,\n  `v2` int DEFAULT NULL,\n  `v3` int DEFAULT NULL,\n  PRIMARY KEY (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" primary_key_columns:"sku" type:"BASE TABLE" data_length:16384
branch_source:name:"product" schema:"CREATE TABLE `product` (\n  `sku` varchar(128) NOT NULL,\n  `description` varchar(128) DEFAULT NULL,\n  `price` bigint DEFAULT NULL,\n  PRIMARY KEY (`sku`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" primary_key_columns:"sku" type:"BASE TABLE" data_length:1589248 row_count:5294
```

##

**PrepareMergeBack**

`PrepareMergeBack` 将构建源数据库和目标数据库之间的差异，然后生成可执行的 DDL 并将其添加到 branch_table_rules 表中。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test PrepareMergeBack

PrepareMergeBack branch_test successfully 
table: product entry: ALTER TABLE `product` ADD COLUMN `v2` int, ADD COLUMN `v3` int
```

## **StartMergeBack**

`StartMergeBack` 将使用 'online' 策略执行 PrepareMergeBack 阶段生成的 DDL。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test StartMergeBack

Start mergeBack branch_test successfully. uuid list:
[c057f330_b1e0_11ee_b7b2_5ea977d56bb7]
```

启动 StartMergeBack 后，我们会收到一个 OnlineDDL 的 UUID。然后可以使用该 UUID 和 `show vitess_migrations like [uuid]` 命令来查询 onlineDDL 的状态。

### **用法**
```sql
mysql> show vitess_migrations like 'c057f330_b1e0_11ee_b7b2_5ea977d56bb7' \G;
```

## **清理**

`Cleanup` 功能将删除与 workflow_name 相关的 `branch_jobs` 表和 `branch_table_rules` 表中的项目。

### **用法**
```shell
vtctlclient --server localhost:15999 Branch -- --workflow_name branch_test cleanup

cleanup workflow:branch_test successfully
```

## **gofakeit 函数**

提供了 `gofakeit_generate` 和 `gofakeit_bytype` 函数，完全兼容 MySQL 的不同类型数据生成。

### **gofakeit_generate**

`gofakeit_generate` 函数遵循 `gofakeit.generate` 的模式生成随机字符串。您可以在 [gofakeit](https://github.com/brianvoe/gofakeit) GitHub 页面上找到更多信息。

### **gofakeit_bytype**

`gofakeit_bytype` 支持以下功能：

| 类型         | 描述                                        | 用法示例                             |
|--------------|---------------------------------------------|--------------------------------------|
| `tinyint`    | 生成一个随机的 tinyint 类型整数。            | `gofakeit_bytype('tinyint')`         |
| `smallint`   | 生成一个随机的 smallint 类型整数。           | `gofakeit_bytype('smallint')`        |
| `mediumint`  | 生成一个随机的 mediumint 类型整数。          | `gofakeit_bytype('mediumint')`       |
| `int`        | 生成一个随机的 int 类型整数。                | `gofakeit_bytype('int')`             |
| `integer`    | `int` 类型的别名。                          | `gofakeit_bytype('integer')`         |
| `bigint`     | 生成一个随机的 bigint 类型整数。             | `gofakeit_bytype('bigint')`          |
| `float`      | 生成一个随机的浮点数。                      | `gofakeit_bytype('float')`           |
| `double`     | 生成一个随机的 double 类型数。               | `gofakeit_bytype('double')`          |
| `decimal`    | 生成一个随机的 decimal 类型数。              | `gofakeit_bytype('decimal', 5, 2)`   |
| `date`       | 生成一个随机日期。                          | `gofakeit_bytype('date')`            |
| `datetime`   | 生成一个随机的日期时间。                    | `gofakeit_bytype('datetime')`        |
| `timestamp`  | 生成一个随机的时间戳。                      | `gofakeit_bytype('timestamp')`       |
| `time`       | 生成一个随机时间。                          | `gofakeit_bytype('time')`            |
| `year`       | 生成一个随机年份。                          | `gofakeit_bytype('year')`            |
| `floatrange` | 生成一个在指定范围内的随机浮点数。          | `gofakeit_bytype('floatrange', 1.5, 10.0)` |
| `intrange`   | 生成一个在指定范围内的随机整数。            | `gofakeit_bytype('intrange', 110, 150)`    |
| `name`       | 生成一个随机名称。                          | `gofakeit_bytype('name')`            |
| `address`    | 生成一个随机地址。                          | `gofakeit_bytype('address')`         |
| `uuid`       | 生成一个随机的 UUID。                       | `gofakeit_bytype('uuid')`            |
| `regex`      | 生成一个符合正则表达式模式的字符串。        | `gofakeit_bytype('regex', '[a-zA-Z]{5}')`  |