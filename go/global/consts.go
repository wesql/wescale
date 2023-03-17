package global

const (
	ApeCloud = "apecloud"
)

// Keyspace
const (
	DefaultKeyspace = "apecloud"
	DefaultShard    = "0"

	// VtDbPrefix + keyspace is the default name for databases.
	VtDbPrefix = "vt_"
	// EmptyDbPrefix is the default database prefix for apecloud databases.
	EmptyDbPrefix = ""
)

// Planner
const (
	UnshardEnabled = true
)
