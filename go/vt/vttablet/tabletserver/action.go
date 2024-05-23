/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package tabletserver

import (
	"github.com/wesql/wescale/go/sqltypes"
	"github.com/wesql/wescale/go/vt/vttablet/tabletserver/rules"
)

type ActionInterface interface {
	BeforeExecution(qre *QueryExecutor) *ActionExecutionResponse

	AfterExecution(qre *QueryExecutor, reply *sqltypes.Result, err error) *ActionExecutionResponse

	ParseParams(stringParams string) (ActionArgs, error)

	SetParams(args ActionArgs) error

	GetRule() *rules.Rule
}

type ActionArgs interface {
	Parse(stringParams string) (ActionArgs, error)
}

type ActionExecutionResponse struct {
	Reply *sqltypes.Result
	Err   error
}
