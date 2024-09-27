package role

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
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
		log.Errorf("unexpected number of rows returned by weSqlProbeStatement: %d\n", len(qr.Rows))
		return "", nil
	}
	if qr.Named() == nil || qr.Named().Row() == nil {
		return "", fmt.Errorf("unexpected result from weSqlProbeStatement: %v", qr)
	}
	role, err := qr.Named().Row().ToString("role")
	return role, err
}
