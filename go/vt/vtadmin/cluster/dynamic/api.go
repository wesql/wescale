package dynamic

import (
	"net/http"

	"github.com/wesql/wescale/go/vt/vtadmin/cluster"

	vtadminpb "github.com/wesql/wescale/go/vt/proto/vtadmin"
)

// API is the interface dynamic APIs must implement.
// It is implemented by vtadmin.API.
type API interface {
	vtadminpb.VTAdminServer
	WithCluster(c *cluster.Cluster, id string) API
	Handler() http.Handler
}
