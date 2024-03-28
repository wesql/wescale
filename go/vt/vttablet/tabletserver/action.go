/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package tabletserver

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

const DefaultPriority = 1000

type ActionInterface interface {
	BeforeExecution(qre *QueryExecutor) error

	AfterExecution(reply *sqltypes.Result, err error) *ActionExecutionResponse

	GetRule() *rules.Rule
}

type ActionExecutionResponse struct {
	FireNext bool
	Reply    *sqltypes.Result
	Err      error
}
