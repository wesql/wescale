/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"sync"

	"vitess.io/vitess/go/mysql"
)

// LastSeenGtid is used to track the last seen gtid
type LastSeenGtid struct {
	mu      sync.RWMutex
	flavor  string
	gtidSet mysql.GTIDSet
}

// NewLastSeenGtid creates a new LastSeenGtid
func NewLastSeenGtid(flavor string) (*LastSeenGtid, error) {
	switch flavor {
	case mysql.MariadbFlavorID:
		return &LastSeenGtid{
			flavor:  flavor,
			gtidSet: mysql.MariadbGTIDSet{},
		}, nil
	case mysql.Mysql56FlavorID:
		return &LastSeenGtid{
			flavor:  flavor,
			gtidSet: mysql.Mysql56GTIDSet{},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported flavor: %s", flavor)
	}
}

// AddGtid adds a gtid to the LastSeenGtid
func (g *LastSeenGtid) AddGtid(gtidStr string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	parsedGtid, err := mysql.ParseGTID(g.flavor, gtidStr)
	if err != nil {
		return err
	}
	g.gtidSet = g.gtidSet.AddGTID(parsedGtid)
	return nil
}

func (g *LastSeenGtid) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.gtidSet.String()
}

// Position returns the current position
func (g *LastSeenGtid) Position() mysql.Position {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return mysql.Position{
		GTIDSet: g.gtidSet,
	}
}
