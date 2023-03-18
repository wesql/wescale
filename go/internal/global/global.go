package global

const ApeCloud = "apecloud"

// Keyspace
const (
	DefaultKeyspace = "apecloud"
	DefaultShard    = "0"

	//// VtDbPrefix + keyspace is the default name for databases.
	VtDbPrefix = "vt_"
	//// EmptyDbPrefix is the default database prefix for apecloud databases.
	EmptyDbPrefix = ""
)

var ApeCloudFeaturesEnable = true

// Planner
var (
	ApeCloudDbDDLPlugin = ApeCloudFeaturesEnable
	UnshardEnabled      = ApeCloudFeaturesEnable
	DbPrefix            = func() string {
		if ApeCloudFeaturesEnable {
			return VtDbPrefix
		}
		return EmptyDbPrefix
	}()
)
