Life of A Query
=====================

* [From Client to VtGate](#from-client-to-vtgate)
* [From VtGate to VtTablet](#from-vtgate-to-vttablet)
* [From VtTablet to MySQL](#from-vttablet-to-mysql)
* [Putting it all together](#putting-it-all-together)

A query means a request for information from database and it involves four components in the case of Vitess, including the client application, VtGate, VtTablet and MySQL instance. This doc explains the interaction which happens between and within components.

![](images/life_of_a_query.png)

At a very high level, as the graph shows, first the client sends a query to VtGate. VtGate then resolves the query and routes it to the right VtTablets. For each VtTablet that receives the query, it does necessary validations and passes the query to the underlying MySQL instance. After gathering results from MySQL, VtTablet sends the response back to VtGate. Once VtGate receives responses from VtTablets, it sends the result to the client. In the presence of VtTablet errors, VtGate will retry the query if errors are recoverable and it only fails the query if either errors are unrecoverable or the maximum number of retries has been reached.

## From Client to VtGate

VtGate acts like a MySQL for Client Applications. A client application first sends an SQL query to VtGate. VtGate's server parses SQL string to AST, then calls the planner, router, executor to execute the SQL, and finally return its result back to client.

![](images/ClientToVTGate.png)


## From VtGate to VtTablet

![VTGateToVTTablet](images/VTGateToVTTablet.png)

VTGate implements the MySQL wire protocol, which means it can behave like a MySQL database.
After receiving an SQL from the client and its `vtgate.Execute()` method being invoked, VtGate needs to parse the SQL into AST, then `Planner` will convert the `AST` to an `PLAN`. 
Due to VTGate being a database proxy, the backend is usually mapped to multiple vttablets, and it needs to select one vttablet to execute SQL. We have implemented features such as Read-Write-Split, Read-After-Write-Consistency, Load Balance in the Router module.
During execution stage, the Executor takes the `PLAN` and eventually sends gRPC requests to VTTablet to execute the queries.

## From VtTablet to MySQL
![](images/VTTabletToMySQL.jpg)

Once VtTablet received an gRPC call from VtGate, it does a few checks before passing the query to MySQL. First, it validates the current VtTablet state including the session id, then generates a query plan and applies predefined query rules and does ACL checks. It also checks whether the query hits the row cache and returns the result immediately if so. In addition, VtTablet consolidates duplicate queries from executing simultaneously and shares results between them. 
Finally, VtTablet will acquire a connection from the connection pool and pass the query down to MySQL layer and wait for the result.
The connection pool is the most important component of VTTablet.

## Putting it all together

![](images/PutAllTogether.png)
