## Usage
```bash
# build the mysql-tester binary
make build-mysql-tester

# run the mysql-tester binary
make run-mysql-tester

# run the mysql-tester binary with Program Arguments
make run-mysql-tester args='-port 15306 -path go/test/mysql-tester'

# record the test output to the result file
make run-mysql-tester args='-port 15306 -path go/test/mysql-tester -record'
```

## Program Arguments
```bash
./bin/mysql-tester -h
Usage of ./bin/mysql-tester:
  -all
        run all tests
  -dbName string
        The database name that firstly connect to. (default "mysql")
  -host string
        The host of the TiDB/MySQL server. (default "127.0.0.1")
  -log-level string
        The log level of mysql-tester: info, warn, error, debug. (default "error")
  -params string
        Additional params pass as DSN(e.g. session variable)
  -passwd string
        The password for the user.
  -path string
        The Base Path of testcase. (default ".")
  -port string
        The listen port of TiDB/MySQL server. (default "3306")
  -record
        Whether to record the test output to the result file.
  -reserve-schema
        Reserve schema after each test
  -retry-connection-count int
        The max number to retry to connect to the database. (default 120)
  -user string
        The user for connecting to the database. (default "root")
  -xunitfile string
        The xml file path to record testing results.
        
```

## Add Testcase Best Practice
1. Write a test file under the t directory, using example.test as a reference.
2. Set up a MySQL database, then run the test with the -record parameter to generate an example.result file.
3. Set up a wesql-scale environment, and use example.test and example.result to test the functionality.
