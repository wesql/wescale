/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package evalengine

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestDateFunctionWithNoFormat(t *testing.T) {
	defaultPattern := "2006-01-02"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("date")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(defaultPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	// Check if the result is a valid date in the expected format
	s := string(result.bytes_)
	match, err := regexp.MatchString(`^\d{4}-\d{2}-\d{2}$`, s)
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Expected date to be in format YYYY-MM-DD, got %s", s)
	}

	// Optionally, check if the result type is correct
	if result.type_ != int16(sqltypes.VarChar) {
		t.Errorf("Expected result type to be VarChar, got %d", result.type_)
	}
}

func TestDateFunctionWithCustomFormat(t *testing.T) {
	customPattern := "January 2, 2006"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("date")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(customPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	s := string(result.bytes_)
	match, err := regexp.MatchString(`^[A-Za-z]+ \d{1,2}, \d{4}$`, s)
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Expected date to be in format 'January 2, 2006', got %s", s)
	}
}
func TestDateFunctionWithInvalidFormat(t *testing.T) {
	invalidPattern := "invalid-format"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("date")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(invalidPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	// 假设错误情况下返回空字符串或特定错误信息
	if string(result.bytes_) != invalidPattern {
		t.Errorf("Expected no output for invalid format, but got: %s", string(result.bytes_))
	}
}

func TestGeneratorWithNamePattern(t *testing.T) {
	formatPattern := "{firstname} {lastname}"

	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(formatPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)
	require.NotEqual(t, formatPattern, string(result.bytes_))
	match, err := regexp.MatchString(`^[A-Za-z]+ [A-Za-z]+$`, string(result.bytes_))
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Generated string does not match name pattern")
	}
}

func TestGeneratorWithBSPattern(t *testing.T) {
	formatPattern := "{bs} {moviename}"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(formatPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	require.NotEqual(t, formatPattern, string(result.bytes_))
	match, err := regexp.MatchString(`^.+ .+$`, string(result.bytes_))
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Generated string does not match BS pattern")
	}
}

func TestGeneratorWithAddressPattern(t *testing.T) {
	formatPattern := "{address} {city}, {state}"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(formatPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)
	require.NotEqual(t, formatPattern, string(result.bytes_))

	match, err := regexp.MatchString(`^.+ .+, .+$`, string(result.bytes_))
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Generated string does not match address pattern")
	}
}

func TestGeneratorWithNumberAndCharPattern(t *testing.T) {
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("###-???")},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	match, err := regexp.MatchString(`^\d{3}-[A-Za-z]{3}$`, string(result.bytes_))
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Generated string does not match numeric/character pattern")
	}
}

func TestGeneratorWithMixedDataPattern(t *testing.T) {
	formatPattern := "{beername} - {color}: {number:1,100}"

	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte(formatPattern)},
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)
	require.NotEqual(t, formatPattern, string(result.bytes_))
	match, err := regexp.MatchString(`^.+ - .+: \d+$`, string(result.bytes_))
	if err != nil {
		t.Fatalf("Regex failed: %v", err)
	}
	if !match {
		t.Errorf("Generated string does not match mixed data pattern")
	}
}

func TestGeneratorWithoutPattern(t *testing.T) {
	errorStr := "error,need pattern param"
	env := &ExpressionEnv{}
	args := []EvalResult{
		{type_: int16(sqltypes.VarChar),
			bytes_: []byte("generator")},
		// 不提供模式
	}
	result := &EvalResult{}

	builtinFake{}.call(env, args, result)

	if string(result.bytes_) != errorStr {
		t.Errorf("Expected no output for missing pattern, but got: %s", string(result.bytes_))
	}
}
