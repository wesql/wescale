# Background
This article discusses two issues: User Authentication and Authorization Management.

`User Authentication`: It addresses the login problem, ensuring that users can log in to the database with a particular "identity".

`Authorization Management`: This deals with permissions, ensuring that with the current "identity", a user can perform Read/Write/Admin operations on a resource.

# User Authentication

wesql-scala offers three types of user management mechanisms: `none`, `static`, and `mysqlbased`.

## none
Bypasses user authentication, so users don’t need to provide usernames or passwords to log into VTGate. This is beneficial for performance when deployed in a secure environment, as it avoids the overhead of authentication.


How to use: 

VTGate startup parameter : 
+ `--mysql_auth_server_impl=none`


## static
Usernames and passwords are stored in a static file. The proxy validates user credentials via this file.
The advantage of using static is that the proxy manages user authentication data. If the proxy backend is connected to multiple MySQL clusters, a single static file can solve the authentication problem. (However, currently, the proxy only supports connecting to one MySQL cluster, so compared to "mysqlbased", it offers no advantage.)

How to use:

VTGate startup parameters :
+ `--mysql_auth_server_impl=static`
+ `--mysql_auth_server_static_file`: Path to the user information file. Note that the user information file should be in JSON format and follow the structure:

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
## mysqlbased
The proxy validates user credentials using the mysql.user table.

It supports the mysql_native_password and caching_sha2_password password plugins. When using caching_sha2_password, users must connect to the proxy using UnixSocket or TLS secure connection.
Principle: The proxy is compatible with the MySQL authentication process (wire protocol + algorithm). It periodically loads data from the mysql.user table into VTGate's memory. When users log in to VTGate, it verifies against this data. Throughout the process, the proxy "acts like a MySQL", so it's compatible with most MySQL clients.

In mysqlbased mode, user information comes from the backend MySQL master. Users need to connect to this master to create user accounts. After VTGate updates the corresponding user information, users can log into VTGate.

How to use:

VTGate startup parameters :

+ `--mysql_auth_server_impl=mysqlbased`: Set the validation mode to mysqlbased.
+ `--mysql_server_require_secure_transport`: If using caching_sha2_passwod, set this parameter.
+ `--mysql_server_ssl_key`: Configure the SSL key. If using caching_sha2_passwod, set this parameter.
+ `--mysql_server_ssl_cert`: Configure the SSL cert. If using caching_sha2_passwod, set this parameter.

# Authorization Management
Currently, wesql-scala categorizes users internally into three types:

`Reader`: Pertains to read DML, e.g., SELECT.

`Writer`: Relates to write DML, e.g., INSERT, UPDATE, DELETE.

`Admin`: Pertains to DDL, e.g., ALTER TABLE.

wesql-scala's authorization is carried out at the vt tablet level and supports both simple and mysqlbased methods.

## simple
Recommended for use with the “static” authentication method. This is a native feature of vitess.
User permission data is stored in a static file. The proxy verifies user permissions through this file.

How to use:

VTTablet startup parameters:

+ `--queryserver_config_strict_table_acl`: If set to true, it enables permission checking.
+ `--table_acl_config_mode=simple`: Specify authentication mode as simple. The parameter also supports mysqlbased.
+ `--table_acl_config`: Path to the static file in JSON format, structured as:


```json
{
  "table_groups": [
    {
      "name": "aclname",
      "table_names_or_prefixes": ["%" ],
      "readers": ["vtgate-user1" ],
      "writers": ["vtgate-user2"],
      "admins": ["vtgate-user3"]
    },
    { "... more ACLs here if necessary ..." }
  ]
}
```
- `--enforce_tableacl_config`: If enabled, it forcefully checks the format of the JSON specified in the `table_acl_config` file. If parsing fails, it terminates the vttablet program.
- `--table_acl_config_reload_interval=30s`: The interval for reloading the static file, default is 30s.
##  mysqlbased

> Recommended for use with the “mysqlbased” authentication method.


The proxy checks user permissions using MySQL's permission information.

In mysqlbased mode, users control permissions by connecting to the MySQL backend. After waiting for the tablet to sync the permission information, it can be applied. In vttablet, roles are abstracted into reader, writer, and admin. If only partial write permissions are granted (e.g., only insert), it will not be considered as a writer.

`Reader`: Users with select permissions are considered readers.

`Writer`: Users with insert, update, and delete permissions are considered writers.

`Admin`: For global (.) permissions, only super users are considered admins. For database-level ({database.*}) and table-level ({database}.{table}) permissions, "all privileges" are required to be considered as admin.

Examples of granting the user the admin role:
```sql
GRANT SUPER ON *.* TO 'user'@'localhost' # global-level
GRANT ALL PRIVILEGES ON test.* TO 'user'@'localhost' # database-level
GRANT ALL PRIVILEGES ON test.table TO 'user'@'localhost' # table-level
```
How to use:

VTTablet startup parameters

+ `--queryserver_config_strict_table_acl`: If set to true, it enables permission checking.
+ `--table_acl_config_reload_interval=30s`: Interval for reloading user information from the MySQL table.
+ `--table_acl_config_mode=mysqlbased`: Specify authentication mode as mysqlbased.