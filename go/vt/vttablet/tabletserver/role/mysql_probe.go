package role

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/dbconfigs"
)

var (
	mysqlDbConfigs *dbconfigs.DBConfigs
)

// mysqlProbe determines the role of a MySQL instance.
// Possible roles: "PRIMARY", "FOLLOWER", ", "UNKNOWN".
func mysqlProbe(ctx context.Context) (string, error) {
	connector := mysqlDbConfigs.AllPrivsConnector()
	conn, err := connector.Connect(ctx)
	if err != nil {
		return UNKNOWN, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer conn.Close()

	checkGroupReplication := false
	// If enabled, check for Group Replication role
	if checkGroupReplication {
		role, err := detectGroupReplicationRole(conn)
		if err != nil {
			return UNKNOWN, err
		}
		if role != "" {
			return role, nil
		}
	}

	// Check if the instance is a replica (slave)
	isReplica, err := checkIfReplica(conn)
	if err != nil {
		return UNKNOWN, err
	}
	if isReplica {
		readOnly, err := isReadOnly(conn)
		if err != nil {
			return UNKNOWN, err
		}
		if readOnly {
			// Map to RDONLY
			return FOLLOWER, nil
		}
		// Map to REPLICA
		return FOLLOWER, nil
	}

	// Check if the instance is a master or standalone
	readOnly, err := isReadOnly(conn)
	if err != nil {
		return UNKNOWN, err
	}

	if !readOnly {
		// The instance is a MASTER or standalone
		return PRIMARY, nil
	}

	// If none of the above, return UNKNOWN
	return UNKNOWN, nil
}

// checkIfReplica checks if the instance is configured as a replica.
func checkIfReplica(conn *mysql.Conn) (bool, error) {
	qr, err := conn.ExecuteFetch("SHOW SLAVE STATUS", 1, true)
	if err != nil {
		return false, fmt.Errorf("failed to execute SHOW SLAVE STATUS: %v", err)
	}
	return len(qr.Rows) > 0, nil
}

// isReadOnly checks if the instance is in read-only mode.
func isReadOnly(conn *mysql.Conn) (bool, error) {
	readOnly, err := getGlobalVariable(conn, "read_only")
	if err != nil {
		return false, err
	}

	superReadOnly, err := getGlobalVariable(conn, "super_read_only")
	if err != nil {
		return false, err
	}

	return readOnly == "ON" || superReadOnly == "ON", nil
}

// detectGroupReplicationRole checks the Group Replication role of the instance.
func detectGroupReplicationRole(conn *mysql.Conn) (string, error) {
	// Check if Group Replication is running
	groupName, err := getGlobalVariable(conn, "group_replication_group_name")
	if err != nil || groupName == "" {
		// Not using Group Replication
		return "", nil
	}

	// Check if Single-Primary Mode is enabled
	singlePrimaryMode, err := getGlobalVariable(conn, "group_replication_single_primary_mode")
	if err != nil {
		return "", fmt.Errorf("failed to get group_replication_single_primary_mode: %v", err)
	}

	// Get the member role and state
	query := "SELECT MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid"
	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil || len(qr.Rows) == 0 {
		return "", fmt.Errorf("failed to get group replication member info: %v", err)
	}
	memberRole := qr.Rows[0][0].ToString()
	memberState := qr.Rows[0][1].ToString()

	if memberState != "ONLINE" {
		return "", nil
	}

	if singlePrimaryMode == "ON" {
		if memberRole == "PRIMARY" {
			return "MASTER", nil
		} else if memberRole == "SECONDARY" {
			return "REPLICA", nil
		}
	} else {
		// Multi-Primary Mode is not supported; return UNKNOWN
		return "", nil
	}

	return "", nil
}

// getGlobalVariable fetches the value of a MySQL global system variable.
func getGlobalVariable(conn *mysql.Conn, variable string) (string, error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)

	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil {
		return "", fmt.Errorf("failed to get variable %s: %v", variable, err)
	}
	if len(qr.Rows) == 0 {
		return "", nil // Variable not found
	}
	value := qr.Rows[0][1].ToString()
	return value, nil
}
