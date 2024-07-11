/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package evalengine

import "vitess.io/vitess/go/sqltypes"

type MyAdd struct{}

func (f MyAdd) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	return sqltypes.Int64, flagIntegerRange
}

func (f MyAdd) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) != 2 {
		throwArgError("myadd function need two parameters")
	}
	p0 := args[0].int64()
	p1 := args[1].int64()
	rst := p0 + p1
	result.setInt64(rst)
}
