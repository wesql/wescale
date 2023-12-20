/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package evalengine

import (
	"fmt"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

type builtinGofakeitByType struct{}

func (builtinGofakeitByType) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) < 1 {
		throwArgError("not found function param")
	}
	if !sqltypes.IsText(args[0].typeof()) {
		throwArgError("first param type is not string")
	}
	funcName := args[0].string()
	switch strings.ToLower(funcName) {
	case "tinyint":
		result.setInt64(int64(gofakeit.Int8()))
	case "smallint":
		result.setInt64(int64(gofakeit.Int16()))
	case "mediumint", "int", "integer":
		result.setInt64(int64(gofakeit.Int32()))
	case "bigint":
		result.setInt64(gofakeit.Int64())
	case "float":
		result.setFloat(float64(gofakeit.Float32()))
	case "double":
		result.setFloat(gofakeit.Float64())
	case "decimal":
		if !sqltypes.IsIntegral(args[1].typeof()) || !sqltypes.IsIntegral(args[2].typeof()) {
			throwArgError("decimal parameters type only support int")
		}
		precision := int32(args[1].int64())
		scala := int32(args[2].int64())
		result.setDecimal(decimal.RandomDecimal(precision, scala), scala)
	case "date":
		formatPattern := "2006-01-02"
		if len(args) == 2 {
			formatPattern = args[1].string()
		}
		var date time.Time
		if strings.ToLower(formatPattern) == "now" {
			date = time.Now()
		} else {
			date = gofakeit.Date()
		}
		formattedDate := date.Format(formatPattern)
		err := result.setValue(sqltypes.MakeTrusted(sqltypes.Date, []byte(formattedDate)), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
		if err != nil {
			throwEvalError(err)
		}
	case "datetime":
		formatPattern := "2006-01-02 00:00:00"
		if len(args) == 2 {
			formatPattern = args[1].string()
		}
		var date time.Time
		if strings.ToLower(formatPattern) == "now" {
			date = time.Now()
		} else {
			date = gofakeit.Date()
		}
		formattedDate := date.Format(formatPattern)
		err := result.setValue(sqltypes.MakeTrusted(sqltypes.Datetime, []byte(formattedDate)), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
		if err != nil {
			throwEvalError(err)
		}
	case "timestamp":
		formatPattern := "2006-01-02 00:00:00"
		if len(args) == 2 {
			formatPattern = args[1].string()
		}
		var date time.Time
		if strings.ToLower(formatPattern) == "now" {
			date = time.Now()
		} else {
			date = gofakeit.Date()
		}
		formattedDate := date.Format(formatPattern)
		err := result.setValue(sqltypes.MakeTrusted(sqltypes.Timestamp, []byte(formattedDate)), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
		if err != nil {
			throwEvalError(err)
		}
	case "time":
		formatPattern := "00:00:00"
		if len(args) == 2 {
			formatPattern = args[1].string()
		}
		var date time.Time
		if strings.ToLower(formatPattern) == "now" {
			date = time.Now()
		} else {
			date = gofakeit.Date()
		}
		formattedDate := date.Format(formatPattern)
		err := result.setValue(sqltypes.MakeTrusted(sqltypes.Time, []byte(formattedDate)), collations.TypedCollation{
			Collation:    collations.CollationUtf8ID,
			Coercibility: collations.CoerceImplicit,
			Repertoire:   collations.RepertoireASCII,
		})
		if err != nil {
			throwEvalError(err)
		}
	case "year":
		result.setInt64(int64(gofakeit.Year()))
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
	}
}
func (builtinGofakeitByType) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
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
	case "name", "address", "uuid", "regex", "date":
		return sqltypes.VarChar, flagIntegerCap
	case "tinyint":
		return sqltypes.Int8, flagIntegerRange
	case "smallint":
		return sqltypes.Int16, flagIntegerRange
	case "mediumint", "int", "integer", "intrange":
		return sqltypes.Int32, flagIntegerRange
	case "bigint":
		return sqltypes.Int64, flagIntegerRange
	case "float":
		return sqltypes.Float32, flagNullable
	case "double", "floatrange":
		return sqltypes.Float64, flagNullable

	}
	throwEvalError(fmt.Errorf("function %v is not support to get type", funcName))
	return sqltypes.VarChar, flagIntegerCap
}

type builtinGofakeitGenerate struct{}

func (builtinGofakeitGenerate) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) < 1 {
		throwArgError("regex function need pattern function")
	}
	var pattern string
	if len(args) == 1 {
		pattern = args[0].string()
	}
	result.setString(gofakeit.Generate(pattern), collations.TypedCollation{
		Collation:    collations.CollationUtf8ID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireASCII,
	})
}
func (builtinGofakeitGenerate) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	return sqltypes.VarChar, flagIntegerCap
}
