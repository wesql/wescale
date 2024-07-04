package engine

import (
	"errors"
	"strconv"
)

type CustomFunctionType func([]string) (string, error)

var CUSTOM_FUNCTIONS map[string]CustomFunctionType = map[string]CustomFunctionType{"myadd": myadd}

func myadd(parameters []string) (string, error) {
	if len(parameters) != 2 {
		return "", errors.New("myadd: should have two int parameters")
	}
	num1, err := strconv.Atoi(parameters[0])
	if err != nil {
		return "", errors.New("myadd: first parameter should be int")
	}
	num2, err := strconv.Atoi(parameters[1])
	if err != nil {
		return "", errors.New("myadd: second parameter should be int")
	}

	return strconv.Itoa(num1 + num2), nil
}
