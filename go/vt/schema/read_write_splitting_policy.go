/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	"fmt"
	"regexp"
)

var (
	readWriteSplittingPolicyParserRegexp = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
)

type ReadWriteSplittingPolicy string

const (
	// ReadWriteSplittingPolicyDisable disables read write splitting
	ReadWriteSplittingPolicyDisable ReadWriteSplittingPolicy = "disable"
	// ReadWriteSplittingPolicyRandom enables read write splitting using random policy
	ReadWriteSplittingPolicyRandom ReadWriteSplittingPolicy = "random"
)

// IsRandom returns true if the strategy is random
func (s ReadWriteSplittingPolicy) IsRandom() bool {
	return s == ReadWriteSplittingPolicyRandom
}

func (s ReadWriteSplittingPolicy) IsDisable() bool {
	return s == ReadWriteSplittingPolicyDisable || s == ""
}

type ReadWriteSplittingPolicySetting struct {
	Strategy ReadWriteSplittingPolicy `json:"strategy,omitempty"`
	Options  string                   `json:"options,omitempty"`
}

func NewReadWriteSplittingPolicySettingSetting(strategy ReadWriteSplittingPolicy, options string) *ReadWriteSplittingPolicySetting {
	return &ReadWriteSplittingPolicySetting{
		Strategy: strategy,
		Options:  options,
	}
}

func ParseReadWriteSplittingPolicySetting(strategyVariable string) (*ReadWriteSplittingPolicySetting, error) {
	setting := &ReadWriteSplittingPolicySetting{}
	strategyName := strategyVariable
	if submatch := readWriteSplittingPolicyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := ReadWriteSplittingPolicy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = ReadWriteSplittingPolicyDisable
	case ReadWriteSplittingPolicyRandom, ReadWriteSplittingPolicyDisable:
		setting.Strategy = strategy
	default:
		return nil, fmt.Errorf("Unknown ReadWriteSplittingPolicy: '%v'", strategy)
	}
	return setting, nil
}

// ToString returns a simple string representation of this instance
func (setting *ReadWriteSplittingPolicySetting) ToString() string {
	return fmt.Sprintf("ReadWriteSplittingPolicySetting: strategy=%v, options=%s", setting.Strategy, setting.Options)
}
