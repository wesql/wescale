package planbuilder

import (
	"github.com/wesql/wescale/go/internal/global"
	"github.com/wesql/wescale/go/vt/key"
	"github.com/wesql/wescale/go/vt/vterrors"
	"github.com/wesql/wescale/go/vt/vtgate/engine"
	"github.com/wesql/wescale/go/vt/vtgate/planbuilder/plancontext"
)

func buildWescaleFilterPlan(query string, vschema plancontext.VSchema) (*planResult, error) {
	dest, ks, _, err := vschema.TargetDestination(global.DefaultKeyspace)
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}
	return newPlanResult(send), nil
}
