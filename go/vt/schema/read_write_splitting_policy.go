/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package schema

import (
	"fmt"
	"regexp"
	"strings"

	querypb "github.com/wesql/wescale/go/vt/proto/query"
)

var (
	readWriteSplittingPolicyParserRegexp = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
)

type ReadWriteSplittingPolicy string

func NewReadWriteSplittingPolicy(s string) ReadWriteSplittingPolicy {
	return ReadWriteSplittingPolicy(strings.ToLower(s))
}

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
	// ReadWriteSplittingPolicyLeastMysqlConnectedConnections enables read write splitting using the least connected connections to mysqld policy
	ReadWriteSplittingPolicyLeastMysqlConnectedConnections ReadWriteSplittingPolicy = "least_mysql_connected_connections"
	// ReadWriteSplittingPolicyLeastMysqlRunningConnections enables read write splitting using the least running connections to mysqld policy
	ReadWriteSplittingPolicyLeastMysqlRunningConnections ReadWriteSplittingPolicy = "least_mysql_running_connections"
	// ReadWriteSplittingPolicyLeastTabletInUseConnections enables read write splitting using the least in-use connections used by vttablet policy
	ReadWriteSplittingPolicyLeastTabletInUseConnections ReadWriteSplittingPolicy = "least_tablet_inuse_connections"
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

func ParseReadWriteSplittingPolicySetting(strategyVariable string) (*ReadWriteSplittingPolicySetting, error) {
	setting := &ReadWriteSplittingPolicySetting{}
	strategyName := strategyVariable
	if submatch := readWriteSplittingPolicyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := NewReadWriteSplittingPolicy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = ReadWriteSplittingPolicyDisable
	case ReadWriteSplittingPolicyRandom,
		ReadWriteSplittingPolicyLeastGlobalQPS,
		ReadWriteSplittingPolicyLeastQPS,
		ReadWriteSplittingPolicyLeastRT,
		ReadWriteSplittingPolicyLeastBehindPrimary,
		ReadWriteSplittingPolicyDisable,
		ReadWriteSplittingPolicyLeastMysqlConnectedConnections,
		ReadWriteSplittingPolicyLeastMysqlRunningConnections,
		ReadWriteSplittingPolicyLeastTabletInUseConnections:
		setting.Strategy = strategy
	default:
		return nil, fmt.Errorf("Unknown ReadWriteSplittingPolicy: '%v'", strategy)
	}
	return setting, nil
}

func CheckReadWriteSplittingRate(ratio int32, strategy string) error {
	if NewReadWriteSplittingPolicy(strategy) == ReadWriteSplittingPolicyDisable {
		return fmt.Errorf("read write splitting policy is not set")
	}
	return CheckReadWriteSplittingRateRange(ratio)
}

func CheckReadWriteSplittingRateRange(ratio int32) error {
	if ratio < 0 || ratio > 100 {
		return fmt.Errorf("read write splitting ratio out of range")
	}
	return nil
}

// ToString returns a simple string representation of this instance
func (setting *ReadWriteSplittingPolicySetting) ToString() string {
	return fmt.Sprintf("ReadWriteSplittingPolicySetting: strategy=%v, options=%s", setting.Strategy, setting.Options)
}

func ToLoadBalancePolicy(s string) querypb.ExecuteOptions_LoadBalancePolicy {
	strategy := NewReadWriteSplittingPolicy(s)
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
	case ReadWriteSplittingPolicyLeastMysqlConnectedConnections:
		return querypb.ExecuteOptions_LEAST_MYSQL_CONNECTED_CONNECTIONS
	case ReadWriteSplittingPolicyLeastMysqlRunningConnections:
		return querypb.ExecuteOptions_LEAST_MYSQL_RUNNING_CONNECTIONS
	case ReadWriteSplittingPolicyLeastTabletInUseConnections:
		return querypb.ExecuteOptions_LEAST_TABLET_INUSE_CONNECTIONS
	default:
		return querypb.ExecuteOptions_RANDOM
	}
}
