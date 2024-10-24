/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package engine

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*DirectDeclarativeDDL)(nil)

// DirectDeclarativeDDL is an operator to send schema diff DDL queries to the specific keyspace, tabletType and destination
type DirectDeclarativeDDL struct {
	tableName     string
	originSchema  string
	desiredSchema string
	diffs         []string

	//  Executed diff DDls to be by Send primitive
	sends []*Send
}

// NeedsTransaction implements the Primitive interface
func (d *DirectDeclarativeDDL) NeedsTransaction() bool {
	return false
}

// RouteType implements Primitive interface
func (d *DirectDeclarativeDDL) RouteType() string {
	return "Send"
}

// GetKeyspaceName implements Primitive interface
func (d *DirectDeclarativeDDL) GetKeyspaceName() string {
	if d.sends == nil || len(d.sends) == 0 || d.sends[0].Keyspace == nil {
		return ""
	}
	return d.sends[0].Keyspace.Name
}

// GetTableName implements Primitive interface
func (d *DirectDeclarativeDDL) GetTableName() string {
	return d.tableName
}

// TryExecute implements Primitive interface
func (d *DirectDeclarativeDDL) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	executedDDL := ""

	for i, send := range d.sends {
		_, err := vcursor.ExecutePrimitive(ctx, send, bindVars, wantfields)
		if err != nil {
			diffDDLs := ""
			for _, ddl := range d.diffs {
				diffDDLs += ddl + ";"
			}
			return nil, fmt.Errorf("the diff DDLs to execute is %v, "+
				"some of them have been executed successfully: %v, "+
				"the failed one is %v: %v", diffDDLs, executedDDL, d.diffs[i], err)
		}
		executedDDL += d.diffs[i] + ";"
	}

	// build final result
	fileNames := []string{"DDL", "Result"}
	var resultRows []sqltypes.Row
	for i := range d.diffs {
		row := sqltypes.BuildVarCharRow(d.diffs[i], "succeed")
		resultRows = append(resultRows, row)
	}

	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(fileNames...),
		Rows:   resultRows,
	}, nil
}

// TryStreamExecute implements Primitive interface
func (d *DirectDeclarativeDDL) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	executedDDL := ""

	for i, send := range d.sends {
		err := vcursor.StreamExecutePrimitive(ctx, send, bindVars, wantfields, func(qr *sqltypes.Result) error {
			// we don't need the query result of each send, so we just ignore it
			return nil
		})
		if err != nil {
			diffDDLs := ""
			for _, ddl := range d.diffs {
				diffDDLs += ddl + ";"
			}
			return fmt.Errorf("the diff DDLs to execute is %v, "+
				"some of them have been executed successfully: %v, "+
				"the failed one is %v: %v", diffDDLs, executedDDL, d.diffs[i], err)
		}
		executedDDL += d.diffs[i] + ";"
	}

	fileNames := []string{"DDL", "Result"}
	var resultRows []sqltypes.Row
	for i := range d.diffs {
		row := sqltypes.BuildVarCharRow(d.diffs[i], "succeed")
		resultRows = append(resultRows, row)
	}

	qr := &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields(fileNames...),
		Rows:   resultRows,
	}
	return callback(qr)
}

// GetFields implements Primitive interface
func (d *DirectDeclarativeDDL) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields: sqltypes.BuildVarCharFields("DDL", "Result"),
	}, nil
}

// Inputs implements the Primitive interface
func (d *DirectDeclarativeDDL) Inputs() []Primitive {
	rst := make([]Primitive, len(d.sends))
	for i, send := range d.sends {
		rst[i] = send
	}
	return rst
}

// description implements the Primitive interface
func (d *DirectDeclarativeDDL) description() PrimitiveDescription {
	var rst PrimitiveDescription

	for _, send := range d.Inputs() {
		rst.Inputs = append(rst.Inputs, send.description())
	}

	rst.OperatorType = "DirectDeclarativeDDL"
	return rst
}
