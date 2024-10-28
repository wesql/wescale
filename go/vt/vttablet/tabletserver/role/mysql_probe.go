package role

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/vt/dbconfigs"
)

var (
	mysqlProbeEnableMgr = false
)

func init() {
	servenv.OnParseFor("vttablet", registerMySqlProbeFlags)
}

func registerMySqlProbeFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&mysqlProbeEnableMgr, "mysql_probe_enable_mgr", mysqlProbeEnableMgr, "enable mysql probe. default is false")
}

var (
	mysqlDbConfigs *dbconfigs.DBConfigs
)

// mysqlProbe determines the role of a MySQL instance.
// Possible roles: "PRIMARY", "FOLLOWER", "UNKNOWN".
func mysqlProbe(ctx context.Context) (string, error) {
	connector := mysqlDbConfigs.AllPrivsConnector()
	conn, err := connector.Connect(ctx)
	if err != nil {
		return UNKNOWN, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Check if we are in wesql-server by checking for the existence of the wesql_cluster_local table
	// If the table exists, return UNKNOWN
	exists, err := checkTableExists(conn, "information_schema", "wesql_cluster_local")
	if err != nil {
		return UNKNOWN, fmt.Errorf("failed to check for table information_schema.wesql_cluster_local: %v", err)
	}
	if exists {
		return UNKNOWN, fmt.Errorf("mysql probe is not supported on wesql-server")
	}

	// If enabled, check for Group Replication role
	if mysqlProbeEnableMgr {
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
			return RDONLY, nil
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

// checkTableExists checks if a specific table exists in a given schema.
func checkTableExists(conn *mysql.Conn, schema string, table string) (bool, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", schema, table)
	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil {
		return false, err
	}
	count, err := qr.Rows[0][0].ToInt64()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// checkIfReplica checks if the instance is configured as a replica.
func checkIfReplica(conn *mysql.Conn) (bool, error) {
	// First, try "SHOW REPLICA STATUS" (new in MySQL 8.0.22)
	qr, err := conn.ExecuteFetch("SHOW REPLICA STATUS", 1, true)
	if err == nil {
		return len(qr.Rows) > 0, nil
	}

	// Check if the error is due to syntax error (command not recognized)
	if isSyntaxError(err) {
		// Try the deprecated "SHOW SLAVE STATUS"
		qr, err = conn.ExecuteFetch("SHOW SLAVE STATUS", 1, true)
		if err != nil {
			return false, fmt.Errorf("failed to execute SHOW REPLICA STATUS or SHOW SLAVE STATUS: %v", err)
		}
		return len(qr.Rows) > 0, nil
	}

	// If the error is not a syntax error, return it
	return false, fmt.Errorf("failed to execute SHOW REPLICA STATUS: %v", err)
}

// isSyntaxError checks if the error is a syntax error indicating an unrecognized command.
func isSyntaxError(err error) bool {
	mysqlErr, ok := err.(*mysql.SQLError)
	if !ok {
		return false
	}
	// MySQL error code 1064 indicates a syntax error.
	return mysqlErr.Num == 1064
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
		return UNKNOWN, nil
	}

	// Check if Single-Primary Mode is enabled
	singlePrimaryMode, err := getGlobalVariable(conn, "group_replication_single_primary_mode")
	if err != nil {
		return UNKNOWN, fmt.Errorf("failed to get group_replication_single_primary_mode: %v", err)
	}

	// Get the member role and state
	query := "SELECT MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID = @@server_uuid"
	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil || len(qr.Rows) == 0 {
		return UNKNOWN, fmt.Errorf("failed to get group replication member info: %v", err)
	}
	memberRole := qr.Rows[0][0].ToString()
	memberState := qr.Rows[0][1].ToString()

	if memberState != "ONLINE" {
		return UNKNOWN, nil
	}

	if singlePrimaryMode == "ON" {
		if memberRole == "PRIMARY" {
			return PRIMARY, nil
		} else if memberRole == "SECONDARY" {
			return FOLLOWER, nil
		}
	} else {
		// Multi-Primary Mode is not supported; return UNKNOWN
		return UNKNOWN, nil
	}

	return UNKNOWN, nil
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
