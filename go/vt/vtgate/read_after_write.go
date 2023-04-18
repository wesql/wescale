/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"strconv"
	"sync"

	"vitess.io/vitess/go/mysql"
)

// todo earayu need to add testcase
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

// GetLastGtid returns the last gtid for a given target
func (g *LastSeenGtid) GetLastGtid(target string) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	switch g.flavor {
	case mysql.MariadbFlavorID:
		sid, err := mysql.ParseSID(target)
		if err != nil {
			return "", err
		}
		return g.gtidSet.(mysql.Mysql56GTIDSet).LastOf(sid), nil
	case mysql.Mysql56FlavorID:
		domain, err := strconv.ParseUint(target, 10, 32)
		if err != nil {
			return "", err
		}
		return g.gtidSet.(mysql.MariadbGTIDSet).LastOf(uint32(domain)), nil
	default:
		return "", fmt.Errorf("unsupported flavor: %s", g.flavor)
	}
}

func (g *LastSeenGtid) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.gtidSet.String()
}
