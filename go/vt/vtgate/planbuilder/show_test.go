/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGenerateCharsetRows(t *testing.T) {
	rows := make([][]sqltypes.Value, 0, 4)
	rows0 := [][]sqltypes.Value{
		append(sqltypes.BuildVarCharRow(
			"utf8",
			"UTF-8 Unicode",
			"utf8_general_ci"),
			sqltypes.NewInt32(3)),
	}
	rows1 := [][]sqltypes.Value{
		append(sqltypes.BuildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			"utf8mb4_general_ci"),
			sqltypes.NewInt32(4)),
	}
	rows2 := [][]sqltypes.Value{
		append(sqltypes.BuildVarCharRow(
			"utf8",
			"UTF-8 Unicode",
			"utf8_general_ci"),
			sqltypes.NewInt32(3)),
		append(sqltypes.BuildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			"utf8mb4_general_ci"),
			sqltypes.NewInt32(4)),
	}

	testcases := []struct {
		input    string
		expected [][]sqltypes.Value
	}{
		{input: "show charset", expected: rows2},
		{input: "show character set", expected: rows2},
		{input: "show charset where charset like 'foo%'", expected: rows},
		{input: "show charset where charset like 'utf8%'", expected: rows0},
		{input: "show charset where charset = 'utf8'", expected: rows0},
		{input: "show charset where charset = 'foo%'", expected: rows},
		{input: "show charset where charset = 'utf8mb4'", expected: rows1},
	}

	charsets := []string{"utf8", "utf8mb4"}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tc.input)
			require.NoError(t, err)
			match := stmt.(*sqlparser.Show).Internal.(*sqlparser.ShowBasic)
			filter := match.Filter
			actual, err := generateCharsetRows(filter, charsets)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
