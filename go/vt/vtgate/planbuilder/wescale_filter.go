package planbuilder

import (
	"vitess.io/vitess/go/internal/global"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
