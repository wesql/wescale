/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var qSelAllRows = "select table_schema, table_name, create_statement from mysql.views"

// Test will validate create view ddls.
func TestCreateViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in mysql.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("create view vitess_view as select * from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))

	// view already exists. This should fail.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "'vitess_view' already exists")

	// view already exists, but create or replace syntax should allow it to replace the view.
	_, err = client.Execute("create or replace view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in mysql.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("create or replace view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate alter view ddls.
func TestAlterViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	// view does not exist, should FAIL
	_, err := client.Execute("alter view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "Table 'vitess_view' does not exist")

	// create a view.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// view exists, should PASS
	_, err = client.Execute("alter view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in mysql.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("create view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate drop view ddls.
func TestDropViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	// view does not exist, should FAIL
	_, err := client.Execute("drop view vitess_view", nil)
	require.ErrorContains(t, err, "Unknown table 'vttest.vitess_view'")

	// view does not exist, using if exists clause, should PASS
	_, err = client.Execute("drop view if exists vitess_view", nil)
	require.NoError(t, err)

	// create two views.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)
	_, err = client.Execute("create view vitess_view2 as select * from vitess_a", nil)
	require.NoError(t, err)

	// drop vitess_view1, should PASS
	_, err = client.Execute("drop view vitess_view1", nil)
	require.NoError(t, err)

	// drop three views, only vitess_view2 exists. This should FAIL but drops the existing view.
	_, err = client.Execute("drop view vitess_view1, vitess_view2, vitess_view3", nil)
	require.ErrorContains(t, err, "Unknown table 'vttest.vitess_view1,vttest.vitess_view3'")

	// validate ZERO rows in mysql.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Zero(t, qr.Rows)

	// create a view.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)

	// drop three views with if exists clause, only vitess_view1 exists. This should PASS but drops the existing view.
	_, err = client.Execute("drop view if exists vitess_view1, vitess_view2, vitess_view3", nil)
	require.NoError(t, err)

	// validate ZERO rows in mysql.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Zero(t, qr.Rows)
}

// TestViewDDLWithInfrSchema will validate information schema queries with views.
func TestViewDDLWithInfrSchema(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// show create view.
	qr, err := client.Execute("show create table vitess_view", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vitess_view\") VARCHAR(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`eid` AS `eid`,`vitess_a`.`id` AS `id`,`vitess_a`.`name` AS `name`,`vitess_a`.`foo` AS `foo` from `vitess_a`\") VARCHAR(\"utf8mb4\") VARCHAR(\"utf8mb4_general_ci\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// show create view.
	qr, err = client.Execute("describe vitess_view", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"eid\") BLOB(\"bigint\") VARCHAR(\"NO\") BINARY(\"\") BLOB(\"0\") VARCHAR(\"\")] [VARCHAR(\"id\") BLOB(\"int\") VARCHAR(\"NO\") BINARY(\"\") BLOB(\"1\") VARCHAR(\"\")] [VARCHAR(\"name\") BLOB(\"varchar(128)\") VARCHAR(\"YES\") BINARY(\"\") NULL VARCHAR(\"\")] [VARCHAR(\"foo\") BLOB(\"varbinary(128)\") VARCHAR(\"YES\") BINARY(\"\") NULL VARCHAR(\"\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// information schema.
	qr, err = client.Execute("select table_type from information_schema.tables where table_schema = database() and table_name = 'vitess_view'", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[BINARY(\"VIEW\")]]",
		fmt.Sprintf("%v", qr.Rows))
}

// TestViewAndTableUnique will validate that views and tables should have unique names.
func TestViewAndTableUnique(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer func() {
		_, _ = client.Execute("drop view if exists vitess_view", nil)
		_, _ = client.Execute("drop table if exists vitess_view", nil)
	}()

	// create a view.
	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// should error on create table as view already exists with same name.
	_, err = client.Execute("create table vitess_view(id bigint primary key)", nil)
	require.ErrorContains(t, err, "Table 'vitess_view' already exists")

	// drop the view
	_, err = client.Execute("drop view vitess_view", nil)
	require.NoError(t, err)

	// create the table first.
	_, err = client.Execute("create table vitess_view(id bigint primary key)", nil)
	require.NoError(t, err)

	// create view should fail as table already exists with same name.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "Table 'vitess_view' already exists")
}

// TestGetSchemaRPC will validate GetSchema rpc..
func TestGetSchemaRPC(t *testing.T) {
	client := framework.NewClient()

	viewSchemaDef, err := client.GetSchema(querypb.SchemaTableType_VIEWS)
	require.NoError(t, err)
	require.Zero(t, len(viewSchemaDef))

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	_, err = client.Execute("create view vitess_view as select 1 from vitess_a", nil)
	require.NoError(t, err)

	viewSchemaDef, err = client.GetSchema(querypb.SchemaTableType_VIEWS)
	require.NoError(t, err)
	require.Equal(t, "create view vitess_view as select 1 from vitess_a", viewSchemaDef["vitess_view"])
}
