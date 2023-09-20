/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"
)

func TestRewriteTableName(t *testing.T) {
	testcases := []struct {
		in        string
		outstmt   string
		isSkipUse bool
	}{
		{
			in:        `select 12 from dual`,
			outstmt:   `select 12 from dual`,
			isSkipUse: true,
		},
		{
			in:        "select 12 from test.dual",
			outstmt:   "select 12 from test.dual",
			isSkipUse: true,
		},
		{
			in:        "select * from `dual`",
			outstmt:   "select * from dual",
			isSkipUse: true,
		},
		{
			in:        `select * from (select 12) as t`,
			outstmt:   `select * from (select 12 from dual) as t`,
			isSkipUse: true,
		},
		{
			// do not rewrite tableName in selectExpr
			in:        `select d1t1.*, d2t1.* from t1 as d1t1 join d2.t1 as d2t1;`,
			outstmt:   `select d1t1.*, d2t1.* from test.t1 as d1t1 join d2.t1 as d2t1`,
			isSkipUse: true,
		},
		{
			in:        `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1;`,
			outstmt:   `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1`,
			isSkipUse: true,
		},
		{
			in:        `select d1.t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt:   `select d1.t1.*, t1.* from d1.t1 join d2.t1`,
			isSkipUse: true,
		},
		{
			in:        `select t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt:   `select t1.*, t1.* from d1.t1 join d2.t1`,
			isSkipUse: true,
		},
		{
			in:        `select (select d from t2 where d > a), t1.* from t1;`,
			outstmt:   `select (select d from test.t2 where d > a), t1.* from test.t1`,
			isSkipUse: true,
		},
		{
			in:        `SELECT COUNT(DISTINCT(t1.id)), LEFT(err_comment, 256) AS comment FROM t1 LEFT JOIN t2 ON t1.id=t2.id GROUP BY comment;`,
			outstmt:   "select count(distinct t1.id), left(err_comment, 256) as `comment` from test.t1 left join test.t2 on t1.id = t2.id group by `comment`",
			isSkipUse: true,
		},
		{
			in: `SELECT ` +
				`pk AS foo, col_int_key AS foo, (SELECT a FROM t2 WHERE a=t1.pk) AS foo ` +
				`FROM t1 ` +
				`GROUP BY pk, col_int_key, (SELECT a FROM t2 WHERE a=t1.pk) ` +
				`ORDER BY pk, col_int_key, (SELECT a FROM t2 WHERE a=t1.pk);`,
			outstmt: "select pk as foo, col_int_key as foo, (select a from test.t2 where a = t1.pk) as foo " +
				"from test.t1 " +
				"group by pk, col_int_key, (select a from test.t2 where a = t1.pk) " +
				"order by pk asc, col_int_key asc, (select a from test.t2 where a = t1.pk) asc",
			isSkipUse: true,
		},

		{
			// do not rewrite tableName when it is a with
			in:        `with simple_query as (select * from t1) select * from simple_query;`,
			outstmt:   `with simple_query as (select * from t1) select * from simple_query`,
			isSkipUse: false,
		},
		{
			in:        `select * from t1,t2,t3 where t1.a=t2.a AND t2.b=t3.a and t1.b=t3.b order by t1.a,t1.b;`,
			outstmt:   `select * from test.t1, test.t2, test.t3 where t1.a = t2.a and t2.b = t3.a and t1.b = t3.b order by t1.a asc, t1.b asc`,
			isSkipUse: true,
		},
		{
			in: `SELECT c.name, o.order_date, oi.product_name 
				FROM (   SELECT id, name   FROM customers ) c 
				JOIN (   SELECT id, customer_id, order_date   FROM orders ) o 
				ON c.id = o.customer_id 
				JOIN (   SELECT id, order_id, product_name   FROM order_items ) oi 
				ON o.id = oi.order_id 
				UNION 
				SELECT c.name, o.order_date, oi.product_name 
				FROM customers c 
				JOIN orders o 
				ON c.id = o.customer_id 
				JOIN order_items oi ON o.id = oi.order_id 
				WHERE oi.price > (   SELECT AVG(price)   FROM order_items );`,
			outstmt: "select c.`name`, o.order_date, oi.product_name " +
				"from (select id, `name` from test.customers) as c " +
				"join (select id, customer_id, order_date from test.orders) as o " +
				"on c.id = o.customer_id " +
				"join (select id, order_id, product_name from test.order_items) as oi " +
				"on o.id = oi.order_id " +
				"union " +
				"select c.`name`, o.order_date, oi.product_name " +
				"from test.customers as c " +
				"join test.orders as o " +
				"on c.id = o.customer_id " +
				"join test.order_items as oi on o.id = oi.order_id " +
				"where oi.price > (select avg(price) from test.order_items)",
			isSkipUse: true,
		},
		{
			in:        `DELETE FROM t1 ORDER BY date ASC, time ASC LIMIT 1;`,
			outstmt:   "delete from test.t1 order by `date` asc, `time` asc limit 1",
			isSkipUse: true,
		},
		{
			// do not rewrite tableName when there are targets in delete statement
			in:        `DELETE t1, t2 FROM wesql.t1 as t1 INNER JOIN t2 as t2 WHERE t1.c2=t2.c2 or t1.c1=1`,
			outstmt:   `delete t1, t2 from wesql.t1 as t1 join t2 as t2 where t1.c2 = t2.c2 or t1.c1 = 1`,
			isSkipUse: false,
		},
		{
			in:        `delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a;`,
			outstmt:   `delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a`,
			isSkipUse: false,
		},
		{
			in:        `update t1 inner join wesql2.t2 inner join wesql3.t3 set t1.c2 = 4, t2.c2 = 44, t3.c2 = 444`,
			outstmt:   `update test.t1 join wesql2.t2 join wesql3.t3 set t1.c2 = 4, t2.c2 = 44, t3.c2 = 444`,
			isSkipUse: true,
		},
		{
			// do not rewrite tableName in create table
			in:        `create table t1 (a int not null auto_increment primary key, b char(32));`,
			outstmt:   "create table test.t1 (\n\ta int not null auto_increment primary key,\n\tb char(32)\n)",
			isSkipUse: true,
		},
		{
			in:        `drop table t1,t2,t3;`,
			outstmt:   `drop table test.t1, test.t2, test.t3`,
			isSkipUse: true,
		},
		{
			in:        `INSERT INTO t1 VALUES (469713,1,164123,1,164123,1)`,
			outstmt:   `insert into test.t1 values (469713, 1, 164123, 1, 164123, 1)`,
			isSkipUse: true,
		},
		{
			in:        `INSERT t1 SELECT 5,6,30 FROM DUAL ON DUPLICATE KEY UPDATE c=c+100;`,
			outstmt:   `insert into test.t1 select 5, 6, 30 from dual on duplicate key update c = c + 100`,
			isSkipUse: true,
		},
		{
			in:        `insert into t3 select t2.*, d as 'x', d as 'z' from t2;`,
			outstmt:   `insert into test.t3 select t2.*, d as x, d as z from test.t2`,
			isSkipUse: true,
		},
		{
			in:        `insert into d1.t1 values (1);`,
			outstmt:   `insert into d1.t1 values (1)`,
			isSkipUse: true,
		},
		{
			in:        `EXPLAIN SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c='13_characters') FROM t1;`,
			outstmt:   `explain select (select sum(LENGTH(c)) from test.t1 where c = '13_characters') from test.t1`,
			isSkipUse: true,
		},
		{
			in:        `analyze table t1;`,
			outstmt:   "otherread",
			isSkipUse: false,
		},
		{
			in:        "describe t1;",
			outstmt:   "explain t1",
			isSkipUse: false,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)
			newStmt, isSkipUse, err := RewriteTableName(stmt, "test")
			require.NoError(t, err)
			assert.Equal(t, tc.isSkipUse, isSkipUse)
			assert.Equal(t, tc.outstmt, String(newStmt))
		})
	}
}

func TestRewriteTableNameForDDL(t *testing.T) {
	testcases := []struct {
		in        string
		outstmt   string
		isSkipUse bool
	}{
		{
			in:        `drop table t1`,
			outstmt:   `drop table test.t1`,
			isSkipUse: true,
		},
		{
			in:        `create table t1 (c1 int)`,
			outstmt:   "create table test.t1 (\n\tc1 int\n)",
			isSkipUse: true,
		},
		{
			in:        `create table t2 select * from t1;`,
			outstmt:   `create table test.t2 select * from test.t1`,
			isSkipUse: true,
		},
		{
			in:        `create table d2.t2 select * from t1;`,
			outstmt:   `create table d2.t2 select * from test.t1`,
			isSkipUse: true,
		},
		{
			in:        `create table t2 select * from d2.t1;`,
			outstmt:   `create table test.t2 select * from d2.t1`,
			isSkipUse: true,
		},
		{
			in:        `rename table t1 to d2.t2`,
			outstmt:   `rename table test.t1 to d2.t2`,
			isSkipUse: true,
		},
		{
			in:        `truncate table t1`,
			outstmt:   `truncate table test.t1`,
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 (pk INT PRIMARY KEY) ",
			outstmt:   "create table test.t1 (\n\tpk INT primary key\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, PRIMARY KEY(pk)) ",
			outstmt:   "create table test.t1 (\n\tid INT not null primary key,\n\tPRIMARY KEY (pk)\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, name VARCHAR(16) NOT NULL, year YEAR ) PARTITION BY HASH(id) PARTITIONS 2;",
			outstmt:   "create table test.t1 (\n\tid INT not null primary key,\n\t`name` VARCHAR(16) not null,\n\t`year` YEAR\n)\npartition by hash (id) partitions 2",
			isSkipUse: true,
		},
		{
			in:        "create table t1 (b int unsigned not null);",
			outstmt:   "create table test.t1 (\n\tb int unsigned not null\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 (c1 INT, c2 CHAR(32) GENERATED ALWAYS AS (RANDOM_BYTES(32))) PARTITION BY HASH(c1);",
			outstmt:   "create table test.t1 (\n\tc1 INT,\n\tc2 CHAR(32) as (RANDOM_BYTES(32)) virtual\n)\npartition by hash (c1)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t (c1 INT, c2 INT) PARTITION BY RANGE(c1) (PARTITION p1 VALUES LESS THAN(100))",
			outstmt:   "create table test.t (\n\tc1 INT,\n\tc2 INT\n)\npartition by range (c1)\n(partition p1 values less than (100))",
			isSkipUse: true,
		},
		{
			in:        "create table t1  select if(1, 9223372036854775808, 1) i, case when 1 then 9223372036854775808 else 1 end c, coalesce(9223372036854775808, 1) co;",
			outstmt:   "create table test.t1 select if(1, 9223372036854775808, 1) as i, case when 1 then 9223372036854775808 else 1 end as c, coalesce(9223372036854775808, 1) as co from dual",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 ( a int not null, b int not null, c int not null, primary key(a,b)) partition by key (a) partitions 3 ;",
			outstmt:   "create table test.t1 (\n\ta int not null,\n\tb int not null,\n\tc int not null,\n\tprimary key (a, b)\n)\npartition by key (a) partitions 3",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1( a CHAR(20) CHARACTER SET ascii, b VARCHAR(20) CHARACTER SET ascii, c TEXT CHARACTER SET ascii );",
			outstmt:   "create table test.t1 (\n\ta CHAR(20) character set ascii,\n\tb VARCHAR(20) character set ascii,\n\tc TEXT character set ascii\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1( a INTEGER, b BLOB, c BLOB, PRIMARY KEY(a,b(1)),  UNIQUE KEY (a,c(1)));",
			outstmt:   "create table test.t1 (\n\ta INTEGER,\n\tb BLOB,\n\tc BLOB,\n\tPRIMARY KEY (a, b(1)),\n\tUNIQUE KEY (a, c(1))\n)",
			isSkipUse: true,
		},
		{
			in: "CREATE TABLE t1 ( id int(11) NOT NULL auto_increment, token varchar(100) DEFAULT '' NOT NULL, count int(11) DEFAULT '0' NOT NULL, " +
				"qty int(11), phone char(1) DEFAULT '' NOT NULL, timestamp datetime DEFAULT '0000-00-00 00:00:00' NOT NULL, PRIMARY KEY (id), " +
				"KEY token (token(15)), UNIQUE token_2 (token(75),count,phone));",
			outstmt: "create table test.t1 (\n\tid int(11) not null auto_increment,\n\ttoken varchar(100) not null default '',\n\t`count` int(11) not null default '0'," +
				"\n\tqty int(11),\n\tphone char(1) not null default '',\n\t`timestamp` datetime not null default '0000-00-00 00:00:00',\n\tPRIMARY KEY (id),\n\tKEY token (token(15))," +
				"\n\tUNIQUE key token_2 (token(75), `count`, phone)\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t1 (c1 INT, c2 INT, PRIMARY KEY (c1,c2))",
			outstmt:   "create table test.t1 (\n\tc1 INT,\n\tc2 INT,\n\tPRIMARY KEY (c1, c2)\n)",
			isSkipUse: true,
		},
		{
			in:        "CREATE TABLE t2(a CHAR(20) CHARACTER SET ascii COLLATE ascii_general_ci, b VARCHAR(20) CHARACTER SET ascii COLLATE ascii_general_ci, c TEXT CHARACTER SET ascii COLLATE ascii_general_ci );",
			outstmt:   "create table test.t2 (\n\ta CHAR(20) character set ascii collate ascii_general_ci,\n\tb VARCHAR(20) character set ascii collate ascii_general_ci,\n\tc TEXT character set ascii collate ascii_general_ci\n)",
			isSkipUse: true,
		},
		{
			in:        "create table t1 (a int) PARTITION BY RANGE (b) ( PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20))",
			outstmt:   "create table test.t1 (\n\ta int\n)\npartition by range (b)\n(partition p1 values less than (10),\n partition p2 values less than (20))",
			isSkipUse: true,
		},
		{
			in:        "create table t1 (a int) PARTITION BY RANGE (b) ( PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20)) ;",
			outstmt:   "create table test.t1 (\n\ta int\n)\npartition by range (b)\n(partition p1 values less than (10),\n partition p2 values less than (20))",
			isSkipUse: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)
			newStmt, isSkipUse, err := RewriteTableName(stmt, "test")
			require.NoError(t, err)
			assert.Equal(t, tc.isSkipUse, isSkipUse)
			if isSkipUse {
				assert.Equal(t, tc.outstmt, String(newStmt))
			}
		})
	}
}
