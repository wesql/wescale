## Usage Scenario
In wescale, when a user sends a SQL command to vtgate, vtgate will route it to the corresponding vttablets, and vttablets will ultimately send it to MySQL for execution. Vttablets cache the recently executed SQL plans and records their execution status. 

Users can use the `show tablets_plans` command to get the execution information of recently executed SQL commands, helping to troubleshoot performance issues in the application programs.
## Usage Method
1. To get the plan cache information for all vttablets, you can use the `show tablets_plans` command.
```
show tablets_plans
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
| tablet_alias | query_template                                              | plan_type | tables       | query_count | accumulated_time | accumulated_mysql_time | rows_affected | rows_returned | error_count |
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
| zone1-101    | insert into mydb.mytable(`name`, age) values (:vtg1, :vtg2) | Insert    | mydb.mytable | 5           | 9.556459ms       | 5.012709ms             | 5             | 0             | 0           |
| zone1-101    | select * from mydb.mytable                                  | Select    | mydb.mytable | 2           | 1.950542ms       | 533.916µs              | 0             | 9             | 0           |
| zone1-101    | explain mytable                                             | OtherRead |              | 1           | 1.237125ms       | 1.212084ms             | 0             | 3             | 0           |
| zone1-100    | select * from mydb.mytable                                  | Select    | mydb.mytable | 3           | 5.952293ms       | 1.582958ms             | 0             | 15            | 0           |
| zone1-102    | select * from mydb.mytable                                  | Select    | mydb.mytable | 3           | 3.283624ms       | 913.208µs              | 0             | 15            | 0           |
+--------------+-------------------------------------------------------------+-----------+--------------+-------------+------------------+------------------------+---------------+---------------+-------------+
5 rows in set (0.00 sec)
```
`tablet_alias`: Alias of the tablet.

`query_template`: Template of the SQL, SQLs with the same template hit the same plan.

`plan_type`: Type of the plan.

`tables`: Databases and tables involved in the SQL.

`query_count`: Number of times the plan is hit.

`accumulated_time`: The accumulated time spent processing through vttablet and MySQL for this plan.

`accumulated_mysql_time`: The accumulated time spent processing through MySQL for this plan.

`rows_affected`: The total number of rows affected by this plan.

`rows_returned`: The total number of rows returned by this plan.

`error_count`: The total number of errors encountered by this plan.

2. To get the plan cache information of a specific vttablet, use `show tablets_plans like 'tablet_alias'` or `show tablets_plans alias ='tablet_alias'`

`like` can be used with wildcards, for example, `Show tablets_plans like '%102'`.

It should be noted that the plans in the cache will be cleared if you add a `wescale filter`.