package tabletserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertSemicolonToNewline(t *testing.T) {
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
		got := ConvertSemicolonToNewline(tt.input)
		assert.Equal(t, tt.expected, got)
	}
}

func TestUserActionArgsToJson(t *testing.T) {

	test := []struct {
		actionType    string
		args          string
		expectedJSON  string
		expectedError bool
	}{
		{
			actionType:    "CONCURRENCY_CONTROL",
			args:          "max_queue_size=10;max_concurrency=5",
			expectedJSON:  `{"max_queue_size":10,"max_concurrency":5}`,
			expectedError: false,
		},
		{
			actionType:    "CONCURRENCY_CONTROL",
			args:          "max_queue_size=10",
			expectedJSON:  `{"max_queue_size":10,"max_concurrency":0}`,
			expectedError: false,
		},
		{
			// wrong action type
			actionType:    "CONCURRENCY_CONTROL222",
			args:          "max_queue_size=10;max_concurrency=5",
			expectedJSON:  `{"max_queue_size":10,"max_concurrency":5}`,
			expectedError: true,
		},
		{
			// if user input wrong args name, no error, but with default arg value
			actionType:    "CONCURRENCY_CONTROL",
			args:          "maxQueueSize=10; max_concurrency=5",
			expectedJSON:  `{"max_queue_size":0,"max_concurrency":5}`,
			expectedError: false,
		},
		{
			// if user input redundant args name, no error, but with default arg value
			actionType:    "CONCURRENCY_CONTROL",
			args:          "maxQueueSize222=10; max_concurrency=5",
			expectedJSON:  `{"max_queue_size":0,"max_concurrency":5}`,
			expectedError: false,
		},
	}
	for _, tt := range test {
		gotJSON, err := UserActionArgsToJSON(tt.actionType, tt.args)
		if tt.expectedError {
			assert.NotNil(t, err)
			fmt.Printf("%v\n", err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, tt.expectedJSON, gotJSON)
		}
	}
}

func TestUserInputToJSON(t *testing.T) {
	userInput := `a=1; b=  2  ;c="3"; d=4.5 ;e=[6,7] ;f = [[8,9],[10,11,12]] `
	expected := `{"A":1,"B":2,"C":"3","D":4.5,"E":[6,7],"F":[[8,9],[10,11,12]]}`
	type MyStruct struct {
		A int
		B int
		C string
		D float64
		E []int
		F [][]int
	}

	userInputTOML := ConvertSemicolonToNewline(userInput)
	s := &MyStruct{}
	str, err := TOMLToJSON(userInputTOML, s)
	assert.Nil(t, err)
	assert.Equal(t, expected, str)
}
