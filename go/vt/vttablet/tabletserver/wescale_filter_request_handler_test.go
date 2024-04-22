package tabletserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertUserInputToTOML(t *testing.T) {
	test := []struct {
		input    string
		expected string
	}{
		{
			input:    "a=1;b=2",
			expected: "a=1\nb=2",
		},
		{
			input:    "a=1;b=[2,3,4,\"5\"];c=6",
			expected: "a=1\nb=[2,3,4,\"5\"]\nc=6",
		},
		{
			input:    "a=1;b=\"here is a ;\";c=6",
			expected: "a=1\nb=\"here is a ;\"\nc=6",
		},
	}
	for _, tt := range test {
		got := ConvertUserInputToTOML(tt.input)
		assert.Equal(t, tt.expected, got)
	}
}
