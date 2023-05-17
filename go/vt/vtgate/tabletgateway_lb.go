/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"strconv"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

// PickTablet picks one tablet based on the pick tablet algorithm
func (gw *TabletGateway) PickTablet(
	availableTablets []*discovery.TabletHealth,
	options *querypb.ExecuteOptions) (*discovery.TabletHealth, error) {
	if len(availableTablets) == 0 {
		return nil, vterrors.VT14002()
	}

	candidates := make([]*discovery.TabletHealth, 0)

	filtered := gw.filterAdvisorByGTIDThreshold(availableTablets, options)
	if len(filtered) > 0 {
		candidates = append(candidates, filtered...)
		if options != nil {
			// The GTID threshold is satisfied, so we can clear it
			options.ReadAfterWriteGtid = ""
		}
	} else {
		candidates = append(candidates, availableTablets...)
	}

	if len(candidates) == 0 {
		return nil, vterrors.VT14002()
	}
	chosenTablet := gw.loadBalance(candidates, options)
	if chosenTablet == nil {
		return nil, vterrors.VT14002()
	}
	return chosenTablet, nil
}

// filterAdvisorByGTIDThreshold compares the GTID threshold in the ExecuteOptions with the GTID of the available tablets.
// If the GTID threshold is satisfied, it returns the tablets that satisfy the threshold.
func (gw *TabletGateway) filterAdvisorByGTIDThreshold(
	availableTablets []*discovery.TabletHealth,
	options *querypb.ExecuteOptions) []*discovery.TabletHealth {
	filtered := make([]*discovery.TabletHealth, 0)
	if len(availableTablets) == 0 {
		return filtered
	}
	// No GTID threshold
	if options == nil || options.GetReadAfterWriteGtid() == "" {
		return filtered
	}
	requestedPosition, err := mysql.ParsePosition(gw.lastSeenGtid.flavor, options.GetReadAfterWriteGtid())
	if err != nil {
		return filtered
	}
	for _, t := range availableTablets {
		if t.Position.AtLeast(requestedPosition) {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func (gw *TabletGateway) loadBalance(candidates []*discovery.TabletHealth, options *querypb.ExecuteOptions) *discovery.TabletHealth {
	if len(candidates) == 0 {
		return nil
	}
	policy := options.GetLoadBalancePolicy()
	switch policy {
	case querypb.ExecuteOptions_LEAST_GLOBAL_QPS:
		gw.leastGlobalQPSLoadBalancer(candidates)
	case querypb.ExecuteOptions_LEAST_QPS:
		gw.leastQPSLoadBalancer(candidates)
	case querypb.ExecuteOptions_LEAST_RT:
		gw.leastRTLoadBalancer(candidates)
	case querypb.ExecuteOptions_LEAST_BEHIND_PRIMARY:
		gw.leastBehindPrimaryLoadBalancer(candidates)
	case querypb.ExecuteOptions_RANDOM:
		gw.randomLoadBalancer(candidates)
	default:
		gw.randomLoadBalancer(candidates)
	}
	return candidates[0]
}

func (gw *TabletGateway) randomLoadBalancer(candidates []*discovery.TabletHealth) {
	if len(candidates) == 0 {
		return
	}
	// shuffle the tablets to distribute the load
	gw.shuffleTablets(gw.localCell, candidates)
}

func (gw *TabletGateway) leastGlobalQPSLoadBalancer(candidates []*discovery.TabletHealth) {
	if len(candidates) == 0 {
		return
	}
	slices.SortFunc(candidates, func(a, b *discovery.TabletHealth) bool {
		if a.Target.GetCell() == b.Target.GetCell() {
			return a.Stats.GetQps() <= b.Stats.GetQps()
		}
		if a.Target.GetCell() == gw.localCell {
			return true
		}
		if b.Target.GetCell() == gw.localCell {
			return false
		}
		return true
	})
}

func (gw *TabletGateway) leastQPSLoadBalancer(candidates []*discovery.TabletHealth) {
	if len(candidates) == 0 {
		return
	}
	statsMap := gw.CacheStatusMap()
	slices.SortFunc(candidates, func(a, b *discovery.TabletHealth) bool {
		aStats := statsMap[strconv.Itoa(int(a.Tablet.Alias.Uid))]
		bStats := statsMap[strconv.Itoa(int(b.Tablet.Alias.Uid))]
		if aStats == nil || bStats == nil {
			return true
		}
		if a.Target.GetCell() == b.Target.GetCell() {
			return aStats.QPS <= bStats.QPS
		}
		if a.Target.GetCell() == gw.localCell {
			return true
		}
		if b.Target.GetCell() == gw.localCell {
			return false
		}
		return true
	})
}

func (gw *TabletGateway) leastRTLoadBalancer(candidates []*discovery.TabletHealth) {
	if len(candidates) == 0 {
		return
	}
	statsMap := gw.CacheStatusMap()
	slices.SortFunc(candidates, func(a, b *discovery.TabletHealth) bool {
		aStats := statsMap[strconv.Itoa(int(a.Tablet.Alias.Uid))]
		bStats := statsMap[strconv.Itoa(int(b.Tablet.Alias.Uid))]
		if aStats == nil || bStats == nil {
			return true
		}
		if a.Target.GetCell() == b.Target.GetCell() {
			return aStats.AvgLatency <= bStats.AvgLatency
		}
		if a.Target.GetCell() == gw.localCell {
			return true
		}
		if b.Target.GetCell() == gw.localCell {
			return false
		}
		return true
	})
}

func (gw *TabletGateway) leastBehindPrimaryLoadBalancer(candidates []*discovery.TabletHealth) {
	if len(candidates) == 0 {
		return
	}
	slices.SortFunc(candidates, func(a, b *discovery.TabletHealth) bool {
		if a.Target.GetCell() == b.Target.GetCell() {
			return a.Position.AtLeast(b.Position)
		}
		if a.Target.GetCell() == gw.localCell {
			return true
		}
		if b.Target.GetCell() == gw.localCell {
			return false
		}
		return true
	})
}
