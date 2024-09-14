package role

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/dbconfigs"
)

var (
	weSqlProbeStatement = "select role from information_schema.wesql_cluster_local;"
	weSqlDbConfigs      *dbconfigs.DBConfigs
)

func wesqlProbe(ctx context.Context) (string, error) {
	connector := weSqlDbConfigs.AllPrivsConnector()
	conn, err := connector.Connect(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to connect to database: %v", err)
	}
	defer conn.Close()

	qr, err := conn.ExecuteFetch(weSqlProbeStatement, 1, true)
	if err != nil {
		return "", fmt.Errorf("failed to execute weSqlProbeStatement: %v", err)
	}
	if len(qr.Rows) != 1 {
		return "", nil
	}
	role, err := qr.Named().Row().ToString("role")
	return role, err
}
