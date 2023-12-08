/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package wrangler

import (
	"testing"
)

func TestRemoveComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "No Comments",
			input:    "select * from table1",
			expected: "select * from table1",
		},
		{
			name:     "Single Line Comment",
			input:    "select * from table1 -- This is a comment",
			expected: "select * from table1",
		},
		{
			name:     "Multi Line Comment",
			input:    "select * from table1 /* Multi line\nComment */",
			expected: "select * from table1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := removeComments(tt.input)
			if err != nil {
				t.Errorf("removeComments(%s) got unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("removeComments(%s) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
