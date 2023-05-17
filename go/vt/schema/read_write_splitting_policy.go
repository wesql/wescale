/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	"fmt"
	"regexp"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
	// ReadWriteSplittingPolicyLeastGlobalQPS enables read write splitting using least global QPS policy
	ReadWriteSplittingPolicyLeastGlobalQPS ReadWriteSplittingPolicy = "least_global_qps"
	// ReadWriteSplittingPolicyLeastQPS enables read write splitting using least QPS policy
	ReadWriteSplittingPolicyLeastQPS ReadWriteSplittingPolicy = "least_qps"
	// ReadWriteSplittingPolicyLeastRT enables read write splitting using least RT policy
	ReadWriteSplittingPolicyLeastRT ReadWriteSplittingPolicy = "least_rt"
	// ReadWriteSplittingPolicyLeastBehindPrimary enables read write splitting using least behind primary policy
	ReadWriteSplittingPolicyLeastBehindPrimary ReadWriteSplittingPolicy = "least_behind_primary"
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
	strategyVariable = strings.ToLower(strategyVariable)
	setting := &ReadWriteSplittingPolicySetting{}
	strategyName := strategyVariable
	if submatch := readWriteSplittingPolicyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := ReadWriteSplittingPolicy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = ReadWriteSplittingPolicyDisable
	case ReadWriteSplittingPolicyRandom,
		ReadWriteSplittingPolicyLeastGlobalQPS,
		ReadWriteSplittingPolicyLeastQPS,
		ReadWriteSplittingPolicyLeastRT,
		ReadWriteSplittingPolicyLeastBehindPrimary,
		ReadWriteSplittingPolicyDisable:
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

func ToLoadBalancePolicy(s string) querypb.ExecuteOptions_LoadBalancePolicy {
	strategy := ReadWriteSplittingPolicy(strings.ToLower(s))
	switch strategy {
	case ReadWriteSplittingPolicyLeastGlobalQPS:
		return querypb.ExecuteOptions_LEAST_GLOBAL_QPS
	case ReadWriteSplittingPolicyLeastQPS:
		return querypb.ExecuteOptions_LEAST_QPS
	case ReadWriteSplittingPolicyLeastRT:
		return querypb.ExecuteOptions_LEAST_RT
	case ReadWriteSplittingPolicyLeastBehindPrimary:
		return querypb.ExecuteOptions_LEAST_BEHIND_PRIMARY
	case ReadWriteSplittingPolicyRandom:
		return querypb.ExecuteOptions_RANDOM
	default:
		return querypb.ExecuteOptions_RANDOM
	}
}
