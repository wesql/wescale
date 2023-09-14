Injecting FailPoints in WeScale for Enhanced Error Testing
=====================
In prior tests, simulating extreme scenarios proved challenging. 
To facilitate error testing and subsequent onlineDDL development, 
we've incorporated the GitHub tool - [pingcap/failpoint](https://github.com/pingcap/failpoint): 
An implementation for Go's failpoints. This article mainly delves 
into using failpoint for error injection and the process to dynamically 
enable failpoints.

## Procedure
The process of using failpoint is as follows:

+ Register failpoint within the code flow.
+ Use `make failpoint-enable` to generate error code.
+ Activate exceptions via environment variables, failpoint.Enable, or through dynamic activation.
+ Once the exception is activated, corresponding tests can be conducted.
+ Use `make failpoint-disable` to restore the code.


## How to Inject failpoint:
You can inject exceptions into the code by invoking the `failpoint.Inject()` function. 
Let's consider the `create-database-error-on-dbname` failpoint as an example. 
The `create database` statement passes through the `createDatabaseInternal` function, 
where we'll introduce a failpoint:`
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
After injecting this exception, it won't interfere with the primary business logic. To generate error code, 
use the `make failpoint-enable` command:
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
## Activating failpoint:
Three methods currently support failpoint activation:
### Environment Variables 
Activate failpoint via the GO_FAILPOINTS environment variable, e.g.,
```shell
export GO_FAILPOINTS=vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname=return("foo")
```
### failpoint.Enable
Activate failpoints statically within the code:
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
### Using SET for failpoint activation
You can now employ `set @put_failpoint='key=value'` and `set @remove_failpoint='key'` commands. Moreover, 
you can view various failpoint statuses using `show failpoints`, 
making it convenient to draft test cases and debug. 
The interactions in MySQL would look something like this:
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
### Registering failpoint-key (Recommended)
Only registered failpoint-keys can be dynamically activated using SET and SHOW commands. 
This is the preferred method to add failpoints:

+ Define the failpoint-key to be injected.
  For instance, add the corresponding failpoint definition in failpoint_key.go and include it in FailpointTable. Use CreateDatabaseErrorOnDbname as an example, where each key comprises:
  + FullName: Failpoint with package name.
  + Name: Excludes package name.
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
+ Inject this failpoint within your procedure

  When injecting, please use the `FailpointKey` defined in `failpoint_key.go` as the name:
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

By following this guide, you can effectively use failpoints in your Wesql-Scala environment, ensuring precise and controlled error handling during testing and development.
