/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRewriteTableName(t *testing.T) {
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]*querypb.BindVariable
	}{
		{
			in:      `select 12 from dual`,
			outstmt: `select 12 from dual`,
		},
		{
			in:      "select 12 from test.dual",
			outstmt: "select 12 from test.dual",
		},
		{
			in:      "select * from `dual`",
			outstmt: "select * from dual",
		},
		{
			in:      `select * from (select 12) as t`,
			outstmt: `select * from (select 12 from dual) as t`,
		},
		{
			// do not rewrite tableName in selectExpr
			in:      `select d1t1.*, d2t1.* from t1 as d1t1 join d2.t1 as d2t1;`,
			outstmt: `select d1t1.*, d2t1.* from test.t1 as d1t1 join d2.t1 as d2t1`,
		},
		{
			in:      `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1;`,
			outstmt: `select d1.t1.*, d2.t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `select d1.t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt: `select d1.t1.*, t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `select t1.*, t1.* from d1.t1 join d2.t1;`,
			outstmt: `select t1.*, t1.* from d1.t1 join d2.t1`,
		},
		{
			in:      `select (select d from t2 where d > a), t1.* from t1;`,
			outstmt: `select (select d from test.t2 where d > a), t1.* from test.t1`,
		},
		{
			in:      `SELECT COUNT(DISTINCT(t1.id)), LEFT(err_comment, 256) AS comment FROM t1 LEFT JOIN t2 ON t1.id=t2.id GROUP BY comment;`,
			outstmt: "select count(distinct t1.id), left(err_comment, 256) as `comment` from test.t1 left join test.t2 on t1.id = t2.id group by `comment`",
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
		},

		{
			// do not rewrite tableName when it is a with
			in:      `with simple_query as (select * from t1) select * from simple_query;`,
			outstmt: `with simple_query as (select * from t1) select * from simple_query`,
		},
		{
			in:      `select * from t1,t2,t3 where t1.a=t2.a AND t2.b=t3.a and t1.b=t3.b order by t1.a,t1.b;`,
			outstmt: `select * from test.t1, test.t2, test.t3 where t1.a = t2.a and t2.b = t3.a and t1.b = t3.b order by t1.a asc, t1.b asc`,
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
			outbv: map[string]*querypb.BindVariable{},
		},
		{
			in:      `DELETE FROM t1 ORDER BY date ASC, time ASC LIMIT 1;`,
			outstmt: "delete from test.t1 order by `date` asc, `time` asc limit 1",
		},
		{
			// do not rewrite tableName when there are targets in delete statement
			in:      `DELETE t1, t2 FROM wesql.t1 as t1 INNER JOIN t2 as t2 WHERE t1.c2=t2.c2 or t1.c1=1`,
			outstmt: `delete t1, t2 from wesql.t1 as t1 join t2 as t2 where t1.c2 = t2.c2 or t1.c1 = 1`,
		},
		{
			in:      `delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a;`,
			outstmt: `delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a`,
		},
		{
			in:      `update t1 inner join wesql2.t2 inner join wesql3.t3 set t1.c2 = 4, t2.c2 = 44, t3.c2 = 444`,
			outstmt: `update test.t1 join wesql2.t2 join wesql3.t3 set t1.c2 = 4, t2.c2 = 44, t3.c2 = 444`,
		},
		{
			// do not rewrite tableName in create table
			in:      `create table t1 (a int not null auto_increment primary key, b char(32));`,
			outstmt: "create table t1 (\n\ta int not null auto_increment primary key,\n\tb char(32)\n)",
		},
		{
			in:      `drop table t1,t2,t3;`,
			outstmt: `drop table test.t1, test.t2, test.t3`,
		},
		{
			in:      `INSERT INTO t1 VALUES (469713,1,164123,1,164123,1)`,
			outstmt: `insert into test.t1 values (469713, 1, 164123, 1, 164123, 1)`,
		},
		{
			in:      `INSERT t1 SELECT 5,6,30 FROM DUAL ON DUPLICATE KEY UPDATE c=c+100;`,
			outstmt: `insert into test.t1 select 5, 6, 30 from dual on duplicate key update c = c + 100`,
		},
		{
			in:      `insert into t3 select t2.*, d as 'x', d as 'z' from t2;`,
			outstmt: `insert into test.t3 select t2.*, d as x, d as z from test.t2`,
		},
		{
			in:      `insert into d1.t1 values (1);`,
			outstmt: `insert into d1.t1 values (1)`,
		},
		{
			in:      `EXPLAIN SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c='13_characters') FROM t1;`,
			outstmt: `explain select (select sum(LENGTH(c)) from test.t1 where c = '13_characters') from test.t1`,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse(tc.in)
			require.NoError(t, err)
			newStmt, _, err := RewriteTableName(stmt, "test")
			require.NoError(t, err)
			assert.Equal(t, tc.outstmt, String(newStmt))
		})
	}
}
