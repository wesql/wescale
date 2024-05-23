package vexec

import (
	"context"

	querypb "github.com/wesql/wescale/go/vt/proto/query"
)

// Executor should be implemented by any tablet-side structs which accept VExec commands
type Executor interface {
	VExec(ctx context.Context, vx *TabletVExec) (qr *querypb.QueryResult, err error)
}
