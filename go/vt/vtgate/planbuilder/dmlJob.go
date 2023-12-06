/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildAlterDMLJobPlan(query string, vschema plancontext.VSchema) (*planResult, error) {
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("ALTER")
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
