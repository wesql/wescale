---
title: Write a Wasm Plugin In WeScale
---

# Extending WeScale with Wasm Plugins

## Overview

WeScale provides a new way to extend its functionality through **WebAssembly (Wasm) plugins**. While traditional methods like writing Filters allow users to customize behavior, they often require compiling and distributing code alongside the service provider's software, which can be cumbersome for many users.

Wasm plugins offer a more secure, dynamic, and user-friendly approach to extensibility. Similar to how modern browsers allow extensions or scripts to modify behavior, Wasm plugins let you inject custom logic into WeScale without deep integration complexities.


## Prerequisites

- **WeScale Access**: Ensure you have access to a WeScale database instance.
- **Familiar with Filters**: Understanding of WeScale Filters is necessary.

---

## Example Scenario Setup

We'll use a sample environment to demonstrate the creation and usage of filters. Consider the following database and table:

```sql
mysql> create database test_wasm;
mysql> use test_wasm;
mysql> create table test_wasm.t1 (c1 int primary key auto_increment, c2 int);
mysql> insert into test_wasm.t1 values (1, 1);
```

## Step 1: Clone the WeScale Wasm Plugin Template

To simplify the process, we've prepared a template for you to write your Wasm Plugin. You can clone it from
[WeScale-Wasm-Plugin-Template](https://github.com/wesql/wescale-wasm-plugin-template) and follow the instructions
in the `README.md` to write your Wasm Plugin.

- **`main.go`**: The main file where you'll write your plugin code.
- **`examples/`**: Contains example plugins for reference.
- **`Makefile`**: Helps with building and deploying the Wasm binary.
- **`README.md`**: Provides instructions on using the template.

![Template Structure](images/wasm3.png)


## Step 2: Write Your Plugin Code

### Understanding the Plugin Code Structure
You will need to implement `WasmPlugin` interface by defining two functions: `RunBeforeExecution` and `RunAfterExecution`.

The `RunBeforeExecution` function is called before the SQL query is executed, while `RunAfterExecution` is called after the query is executed.
You can intercept or manipulate the SQL query in `RunBeforeExecution` and modify the query result in `RunAfterExecution`.

You also need to define a `main` function to start the plugin.

![Editing main.go](images/wasm4.png)

### Implementing a Custom Wasm Plugin

DML queries without a `WHERE` clause can be dangerous as they can affect all rows in a table. It is a common security practice to prevent such queries from executing.

The output should return an error message if a `WHERE` clause is missing, like so:
```sql
mysql> DELETE FROM db.tbl;
Error from wasm plugin at before execution stage: no where clause

mysql> UPDATE db.tbl SET username = 'Bryce';
Error from wasm plugin at before execution stage: no where clause
```

<details>
<summary>Here's the complete code for the plugin</summary>

https://raw.githubusercontent.com/wesql/wescale-wasm-plugin-template/refs/heads/main/examples/interceptor/main.go

```go
package main

import (
	"fmt"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/go/vt/proto/query"
	"github.com/wesql/wescale-wasm-plugin-sdk/pkg"
	hostfunction "github.com/wesql/wescale-wasm-plugin-sdk/pkg/host_functions"
)

func main() {
	pkg.InitWasmPlugin(&ParserWasmPlugin{})
}

type ParserWasmPlugin struct {
}

func (a *ParserWasmPlugin) RunBeforeExecution() error {
	query, err := hostfunction.GetHostQuery()
	if err != nil {
		return err
	}

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		hostfunction.InfoLog("parse error: " + err.Error())
		return nil
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		if stmt.Where == nil {
			return fmt.Errorf("no where clause")
		}
	case *sqlparser.Delete:
		if stmt.Where == nil {
			return fmt.Errorf("no where clause")
		}
	default:
	}

	return nil
}

func (a *ParserWasmPlugin) RunAfterExecution(queryResult *query.QueryResult, errBefore error) (*query.QueryResult, error) {
	// do nothing
	return queryResult, errBefore
}
```
</details>

Let's break down the code of the plugin:

1. **Import Necessary Packages**:
```go
import (
	"fmt"
	"github.com/wesql/sqlparser"
	"github.com/wesql/sqlparser/go/vt/proto/query"
	"github.com/wesql/wescale-wasm-plugin-sdk/pkg"
	hostfunction "github.com/wesql/wescale-wasm-plugin-sdk/pkg/host_functions"
)
```

Explain:
- `sqlparser`: Library to parse SQL queries. It can be used to check for DML queries without a `WHERE` clause.
- `wescale-wasm-plugin-sdk`: SDK to interact with the WeScale host environment.
- `host_functions`: Functions provided by the host environment. e.g. `hostfunction.GetHostQuery()` returns the current executing SQL query.

2. **Implement `RunBeforeExecution` Method**:

This method intercepts the query before execution.

```go
func (a *ParserWasmPlugin) RunBeforeExecution() error {
   // Retrieve the SQL query using `hostfunction.GetHostQuery()`.
   query, err := hostfunction.GetHostQuery()
   if err != nil {
       return err
   }
   
   // Parse the SQL query into an AST. 
   stmt, err := sqlparser.Parse(query)
   if err != nil {
       hostfunction.InfoLog("parse error: " + err.Error())
       return nil
   }
   
   // Check for DML queries without a WHERE clause.
   // Return an error if a `WHERE` clause is missing.
   switch stmt := stmt.(type) {
   case *sqlparser.Update:
       if stmt.Where == nil {
           return fmt.Errorf("no where clause")
       }
   case *sqlparser.Delete:
       if stmt.Where == nil {
           return fmt.Errorf("no where clause")
       }
   default:
   }
   
   return nil
}
```

3. **Implement `RunAfterExecution` Method**:

Since no action is needed after execution, you can leave it as is.

```go
func (a *CustomWasmPlugin) RunAfterExecution(queryResult *query.QueryResult, errBefore error) (*query.QueryResult, error) {
    return queryResult, errBefore
}
```

4. **Complete the Plugin**:

Ensure your `main.go` includes the `main` function to register the plugin.

```go
func main() {
	pkg.InitWasmPlugin(&ParserWasmPlugin{})
}
```

---

## Step 3: Compile and Deploy the Plugin

### 1. Build the Wasm Binary

Use the `Makefile` to compile your plugin into a Wasm binary:

```bash
make build-wasm-using-docker
```

This command generates the `my_plugin.wasm` file in the `bin/` directory.

### 2. Deploy the Wasm Plugin to WeScale

First, install the `wescale_wasm` binary to help with deployment:

```bash
make install-wescale-wasm
```

Then, deploy your plugin using:

```bash
./bin/wescale_wasm --command=install \
  --wasm_file=./bin/my_plugin.wasm \
  --mysql_host=127.0.0.1 \
  --mysql_port=15306 \
  --mysql_user=root \
  --mysql_password=root \
  --create_filter
```

Replace the MySQL connection details with those of your WeScale instance.

### 3. Verify the Plugin Installation

Check if the plugin is active:


```
MySQL [mysql]> SHOW FILTERS\G
*************************** 1. row ***************************
                         id: 9
           create_timestamp: 2024-09-29 10:23:15
           update_timestamp: 2024-09-29 10:23:15
                       name: my_plugin_wasm_filter
                description:
                   priority: 1000
                     status: ACTIVE
                      plans: ["Select","Insert","Update","Delete"]
fully_qualified_table_names: []
                query_regex:
             query_template:
           request_ip_regex:
                 user_regex:
      leading_comment_regex:
     trailing_comment_regex:
             bind_var_conds:
                     action: WASM_PLUGIN
                action_args: wasm_binary_name="my_plugin.wasm"
```


## Step 4: Test the Plugin

### Example 1: Explain the Filter

Use the `/*explain filter*/` comment to see which filters apply:
> NOTICE: Make sure you are connecting to MySQL using `-c` parameter, otherwise the `/*explain filter*/` comment will be ignored.
```bash
mysql -h127.0.0.1 -P15306 -Ac -e '/*explain filter*/ DELETE FROM test_wasm.t1;'
```

Expected output:

```
+-----------------------+-------------+----------+-------------+-----------------------------------+
| Name                  | description | priority | action      | action_args                       |
+-----------------------+-------------+----------+-------------+-----------------------------------+
| my_plugin_wasm_filter |             | 1000     | WASM_PLUGIN | wasm_binary_name="my_plugin.wasm" |
+-----------------------+-------------+----------+-------------+-----------------------------------+
```

### Example 2: Attempt a DML Without WHERE Clause

Try running a `DELETE` without a `WHERE` clause:

```bash
mysql -h127.0.0.1 -P15306 -Ac -e 'DELETE FROM test_wasm.t1;'
```

Expected error:

```
ERROR 1105 (HY000) at line 1: target: .0.primary: vttablet: rpc error: code = Unknown desc = error from wasm plugin at after execution stage: error from wasm plugin at before execution stage: no where clause (CallerID: userData1)
```

The plugin intercepts and prevents the execution.


## Step 5: Remove the Plugin

To uninstall the plugin:

```bash
./bin/wescale_wasm --command=uninstall --filter_name=my_plugin_wasm_filter
```

Confirm removal:

```bash
mysql -h127.0.0.1 -P15306 -e 'SHOW FILTERS\G'
```

The plugin should no longer be listed.


## Conclusion

You've successfully created and deployed a Wasm plugin in WeScale that enhances security by preventing accidental execution of `UPDATE` or `DELETE` statements without a `WHERE` clause.

Wasm plugins provide a flexible and secure method to extend WeScale's functionality without the complexities of compiling and distributing code alongside the service provider's software.


## Additional Resources

- **WeScale Wasm Plugin Template**: [GitHub Repository](https://github.com/wesql/wescale-wasm-plugin-template)
- **Wasm Plugin Design Document**: [Design Document](https://github.com/wesql/wescale/blob/main/doc/design/20240531_WasmPlugin.md)


## Next Steps

- **Explore More Examples**: Check the `examples/` directory for additional plugins.
- **Customize Further**: Modify your plugin to handle other scenarios or add new features.
- **Contribute**: Share your plugins with the community or contribute to the template repository.

By leveraging Wasm plugins, you can tailor WeScale to better meet your application's specific needs, enhancing both functionality and security.
