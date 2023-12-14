/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package evalengine

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

type builtinFake struct{}

func (builtinFake) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) < 1 {
		throwArgError("not found function param")
	}
	if !sqltypes.IsText(args[0].typeof()) {
		throwArgError("first param type is not string")
	}
	funcName := args[0].string()
	switch strings.ToLower(funcName) {
	case "date":
		formatPattern := "2006-01-02"
		if len(args) == 2 {
			formatPattern = args[1].string()
		}
		date := gofakeit.Date()
		formattedDate := date.Format(formatPattern)
		result.setString(formattedDate, collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	case "int8":
		result.setInt64(int64(gofakeit.Int8()))
	case "int16":
		result.setInt64(int64(gofakeit.Int16()))
	case "int32":
		result.setInt64(int64(gofakeit.Int32()))
	case "int64":
		result.setInt64(gofakeit.Int64())
	case "float32":
		result.setFloat(float64(gofakeit.Float32()))
	case "float64":
		result.setFloat(gofakeit.Float64())
	case "floatrange":
		if len(args) < 3 {
			throwArgError("float32range function need min and max parameters")
		}
		var min, max float64
		if !sqltypes.IsNumber(args[1].typeof()) || !sqltypes.IsNumber(args[2].typeof()) {
			throwArgError("float32range parameters type only support float")
		}
		min, ok := args[1].decimal().Float64()
		if !ok {
			throwArgError("float32range parameters min's type only support float")
		}
		max, ok = args[2].decimal().Float64()
		if !ok {
			throwArgError("float32range parameters min's type only support float")
		}
		result.setFloat(gofakeit.Float64Range(min, max))
	case "intrange":
		if len(args) < 3 {
			throwArgError("float32range function need min and max parameters")
		}
		var min, max int
		if !sqltypes.IsIntegral(args[1].typeof()) || !sqltypes.IsIntegral(args[2].typeof()) {
			throwArgError("float32range parameters type only support int")
		}
		min = int(args[1].int64())
		max = int(args[2].int64())
		result.setInt64(int64(gofakeit.IntRange(min, max)))
	case "name":
		result.setString(gofakeit.Name(), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	case "address":
		result.setString(gofakeit.Address().Address, collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	case "uuid":
		result.setString(gofakeit.UUID(), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	case "regex":
		if len(args) < 2 {
			throwArgError("regex function need pattern function")
		}
		var pattern string
		if len(args) == 2 {
			pattern = args[1].string()
		}
		result.setString(gofakeit.Regex(pattern), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	case "generator":
		if len(args) < 2 {
			result.setString(gofakeit.Generate("error,need pattern param"), collations.TypedCollation{
				Collation:    collations.CollationUtf8ID,
				Coercibility: collations.CoerceImplicit,
				Repertoire:   collations.RepertoireASCII,
			})
			return
		}
		var pattern string
		if len(args) == 2 {
			pattern = args[1].string()
		}
		result.setString(gofakeit.Generate(pattern), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
	}
}
func (builtinFake) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) < 1 {
		throwArgError("not found function param")
	}
	result := EvalResult{}
	args[0].eval(env, &result)
	if !sqltypes.IsText(result.typeof()) {
		throwArgError("first param type is not string")
	}
	funcName := result.string()
	switch strings.ToLower(funcName) {
	case "name", "address", "uuid", "regex", "generator", "date":
		return sqltypes.VarChar, flagIntegerCap
	case "int8":
		return sqltypes.Int8, flagIntegerRange
	case "int16":
		return sqltypes.Int16, flagIntegerRange
	case "int32", "intrange":
		return sqltypes.Int32, flagIntegerRange
	case "int64":
		return sqltypes.Int64, flagIntegerRange
	case "float32":
		return sqltypes.Float32, flagNullable
	case "float64", "floatrange":
		return sqltypes.Float64, flagNullable

	}
	throwEvalError(fmt.Errorf("function %v is not support to get type", funcName))
	return sqltypes.VarChar, flagIntegerCap
}
