/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
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
	// shuffle the tablets to distribute the load
	gw.shuffleTablets(gw.localCell, candidates)
	return candidates[0], nil
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
