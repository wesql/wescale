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

	p0, err := args[0].value().ToInt64()
	if err != nil {
		throwArgError("myadd function deals only integer parameters")
	}

	p1, err := args[1].value().ToInt64()
	if err != nil {
		throwArgError("myadd function deals only integer parameters")
	}

	rst := p0 + p1
	result.setInt64(rst)
}
