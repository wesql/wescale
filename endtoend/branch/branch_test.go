package wasm

import (
	"context"
	"testing"
	"time"
)

func TestSourceAndTargetClusterConnection(t *testing.T) {
	// Create context with 10-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Define retry interval
	retryInterval := 5 * time.Second

	// Channel for completion signal
	done := make(chan bool)

	// Run connection tests in goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				// Timeout or cancelled
				t.Error("Connection test timeout")
				done <- true
				return
			default:
				// Test source cluster connection
				err := sourceCluster.MysqlDb.Ping()
				if err != nil {
					t.Logf("Source cluster MySQL connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				err = sourceCluster.WescaleDb.Ping()
				if err != nil {
					t.Logf("Source cluster Wescale connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				// Test target cluster connection
				err = targetCluster.MysqlDb.Ping()
				if err != nil {
					t.Logf("Target cluster MySQL connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				err = targetCluster.WescaleDb.Ping()
				if err != nil {
					t.Logf("Target cluster Wescale connection failed: %v", err)
					time.Sleep(retryInterval)
					continue
				}

				// All connections successful
				t.Log("All cluster connections test passed")
				done <- true
				return
			}
		}
	}()

	// Wait for test completion or timeout
	<-done
}
