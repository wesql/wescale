---
标题: 在 WeScale 中使用 FailPoint 注入
---

在 WeScale 中注入 FailPoint 以增强错误测试
=====================
在代码测试过程中，模拟极端环境较为困难。
为方便错误测试，我们引入了 GitHub 工具 - [pingcap/failpoint](https://github.com/pingcap/failpoint)：这是一个用于 Go 语言的 FailPoint 实现。本文主要探讨如何使用 FailPoint 进行错误注入以及动态启用 FailPoint 的过程。

## 流程
使用 FailPoint 的过程如下：

+ 在代码流中注册 FailPoint。
+ 使用 `make failpoint-enable` 生成错误代码。
+ 通过环境变量、failpoint.Enable 或动态激活来触发异常。
+ 一旦异常被激活，可以进行相应的测试。
+ 使用 `make failpoint-disable` 恢复代码。

## 如何注入 FailPoint
你可以通过调用 `failpoint.Inject()` 函数将异常注入到代码中。让我们以 `create-database-error-on-dbname` FailPoint 为例。`create database` 语句会使得WeScale调用 `createDatabaseInternal` 函数，在此我们将引入一个 FailPoint：
```go
func createDatabaseInternal(ctx context.Context, ts *topo.Server, f func() error, keyspaceName string, cells []string) error {
    ......
    failpoint.Inject(failpointkey.CreateDatabaseErrorOnDbname.Name, func(v failpoint.Value) {
       if v != nil && v.(string) == keyspaceName {
          failpoint.Return(fmt.Errorf("create-database-error-on-dbname error injected"))
       }
    })
    ......
    return nil
}
```
在注入此异常后，它不会干扰主业务逻辑。要生成错误代码，使用 `make failpoint-enable` 命令：
```go
func createDatabaseInternal(ctx context.Context, ts *topo.Server, f func() error, keyspaceName string, cells []string) error {
    ......
    if v, _err_ := failpoint.Eval(_curpkg_(failpointkey.CreateDatabaseErrorOnDbname.Name)); _err_ == nil {
        if v != nil && v.(string) == keyspaceName {
           return fmt.Errorf("create-database-error-on-dbname error injected")
        }
    }
    ......
    return nil
}
```

## 启用 FailPoint
目前有三种方法支持启用 FailPoint：

### 环境变量
通过 GO_FAILPOINTS 环境变量启用 FailPoint，例如：
```shell
export GO_FAILPOINTS=vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname=return("foo")
```

### failpoint.Enable
在代码中静态启用 FailPoint：
```go
package vtgate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
)

func GetValueOfFailPoint(fpName string) failpoint.Value {
	failpoint.Inject(fpName, func(v failpoint.Value) {
		failpoint.Return(v)
	})
	return 0
}

func TestFailpointEnable(t *testing.T) {
	failpoint.Enable("vitess.io/vitess/go/vt/vtgate/testPanic", "return(1)")
	require.Equal(t, 1, GetValueOfFailPoint("testPanic"))
}
```

### 使用 SET 命令启用 FailPoint
你现在可以使用 `set @put_failpoint='key=value'` 和 `set @remove_failpoint='key'` 命令。此外，你还可以使用 `show failpoints` 查看各种 FailPoint 的状态，这使得编写测试用例和调试更加方便。MySQL 中的交互示例如下：
```shell
mysql> show failpoints;
+------------------------------------------------------------------+---------+
| failpoint keys                                                   | Enabled |
+------------------------------------------------------------------+---------+
| vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname | false   |
| TestFailPointError                                               | false   |
| vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint                   | true    |
+------------------------------------------------------------------+---------+

mysql> set @put_failpoint='TestFailPointError=return(1)';

mysql> show failpoints;
+------------------------------------------------------------------+---------+
| failpoint keys                                                   | Enabled |
+------------------------------------------------------------------+---------+
| vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname | false   |
| TestFailPointError                                               | true    |
| vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint                   | true    |
+------------------------------------------------------------------+---------+

mysql> set @remove_failpoint='TestFailPointError';
Query OK, 0 rows affected
mysql> show failpoints;
+------------------------------------------------------------------+---------+
| failpoint keys                                                   | Enabled |
+------------------------------------------------------------------+---------+
| vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname | false   |
| TestFailPointError                                               | false   |
| vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint                   | true    |
+------------------------------------------------------------------+---------+
```

### 注册 FailPoint Key（推荐）
只有已注册的 FailPoint Key 才能使用 SET 和 SHOW 命令进行动态激活。这是添加 FailPoint 的首选方法：

+ 定义要注入的 FailPoint Key。
  例如，在 failpoint_key.go 中添加相应的 FailPoint 定义，并将其包含在 FailpointTable 中。以 `CreateDatabaseErrorOnDbname` 为例，每个 Key 包含：
    + FullName: 包含包名的 FailPoint 名称。
    + Name: 不包含包名的名称。
  ```go
    var (
        TestFailPointError = FailpointKey{
        FullName: "TestFailPointError",
        Name:     "TestFailPointError",
        }
        CreateDatabaseErrorOnDbname = FailpointKey{
        FullName: "vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname",
        Name:     "create-database-error-on-dbname",
        }
    )
    func init() {
        err := failpoint.Enable("vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint", "return(1)")
        if err != nil {
        return
        }
        FailpointTable = make(map[string]string)
        FailpointTable[CreateDatabaseErrorOnDbname.FullName] = CreateDatabaseErrorOnDbname.Name
        FailpointTable[TestFailPointError.FullName] = TestFailPointError.Name
    }
  ```

+ 在你的流程中注入这个 FailPoint

  注入时，请使用在 `failpoint_key.go` 中定义的 `FailpointKey` 作为名称：
```go
func createDatabaseInternal(ctx context.Context, ts *topo.Server, f func() error, keyspaceName string, cells []string) error {
    ......
    failpoint.Inject(failpointkey.CreateDatabaseErrorOnDbname.Name, func(v failpoint.Value) {
       if v != nil && v.(string) == keyspaceName {
          failpoint.Return(fmt.Errorf("create-database-error-on-dbname error injected"))
       }
    })
    ......
    return nil
}
```

通过遵循本指南，你可以在 WeScale 环境中有效地使用 FailPoint，从而在测试和开发过程中实现精确和可控的错误处理。