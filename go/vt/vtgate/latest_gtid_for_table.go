/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	// "fmt"
	"sync"
	"time"
	// "vitess.io/vitess/go/mysql"
)

// LatestGTIDEntry represents an entry in the LatestGTIDManager with the table name, GTID, and the time it was updated.
type LatestGTIDEntry struct {
	GTID       string
	UpdateTime time.Time
}

// LatestGTIDForTable manages the latest GTID and update time for each table.
type LatestGTIDForTable struct {
	latestGTIDs map[string]LatestGTIDEntry // Key is the table name, value is the LatestGTIDEntry struct.
	expireTime  time.Duration              // The expiration time for GTID entries.
	mu          sync.RWMutex               // Mutex for read-write synchronization.
	wg          sync.WaitGroup             // WaitGroup to wait for the cleanup goroutine to finish.
}

// UpdateGTID updates the latest GTID and update time for a given table.
func (m *LatestGTIDForTable) UpdateGTID(tableName, gtid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestGTIDs[tableName] = LatestGTIDEntry{
		GTID:       gtid,
		UpdateTime: time.Now(),
	}
}

// GetLatestGTID retrieves the latest GTID for a given table.
// If the table is not found or the GTID has expired, it returns an empty string and false.
func (m *LatestGTIDForTable) GetLatestGTID(tableName string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.latestGTIDs[tableName]
	if !ok || time.Now().Sub(entry.UpdateTime) > m.expireTime {
		return "", false
	}
	return entry.GTID, true
}

// startCleaner starts a goroutine to periodically clean up expired GTID entries.
func (m *LatestGTIDForTable) startCleaner() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.expireTime)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.mu.Lock()
				now := time.Now()
				for tableName, entry := range m.latestGTIDs {
					if now.Sub(entry.UpdateTime) > m.expireTime {
						delete(m.latestGTIDs, tableName)
					}
				}
				m.mu.Unlock()
			}
		}
	}()
}

// Stop waits for the cleanup goroutine to finish.
func (m *LatestGTIDForTable) Stop() {
	m.wg.Wait()
}
