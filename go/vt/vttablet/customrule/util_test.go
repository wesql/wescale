package customrule

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserInputStrArrayToArray(t *testing.T) {
	testCases := []struct {
		input       string
		expected    []string
		expectedErr bool
	}{
		{
			input:    `a,b`,
			expected: []string{"a", "b"},
		},
		{
			input:    `1,2`,
			expected: []string{"1", "2"},
		},
		{
			input:    `a b cde ;f g, h`,
			expected: []string{"abcde;fg", "h"},
		},
		{
			input:    ``,
			expected: []string{},
		},
		{
			input:    `,`,
			expected: []string{"", ""},
		},
		{
			input:    `,,`,
			expected: []string{"", "", ""},
		},
	}
	for _, tt := range testCases {
		got, err := UserInputStrArrayToArray(tt.input)

		if tt.expectedErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			for i, v := range got {
				sv := v.(string)
				assert.Equal(t, sv, tt.expected[i])
			}
		}
	}
}
