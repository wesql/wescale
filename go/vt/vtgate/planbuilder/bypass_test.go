/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package planbuilder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func Test_buildPlanForBypass(t *testing.T) {

	vschema := &vschemaWrapper{
		v: loadSchema(t, "vschemas/schema.json", true),
		keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		tabletType: topodatapb.TabletType_PRIMARY,
		dest:       key.DestinationShard("0"),
	}

	type args struct {
		sql     string
		vschema plancontext.VSchema
	}
	type want struct {
		FieldQuery string
	}

	tests := []struct {
		name    string
		args    args
		want    want
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "select * from t1",
			args: args{
				sql:     "select * from t1",
				vschema: vschema,
			},
			want: want{
				FieldQuery: "select * from t1 where 1 != 1",
			},
			wantErr: assert.NoError,
		},
		{
			name: "select * from t1 join t2 on t1.id = t2.id",
			args: args{
				sql:     "select * from t1 join t2 on t1.id = t2.id",
				vschema: vschema,
			},
			want: want{
				FieldQuery: "select * from t1 join t2 on t1.id = t2.id where 1 != 1",
			},
			wantErr: assert.NoError,
		},
		{
			name: "select * from t1 join t2 on t1.id = t2.id where id > 100",
			args: args{
				sql:     "select * from t1 join t2 on t1.id = t2.id where id > 100",
				vschema: vschema,
			},
			want: want{
				FieldQuery: "select * from t1 join t2 on t1.id = t2.id where 1 != 1",
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(tt.args.sql)
			assert.NoError(t, err)
			plan, err := buildPlanForBypass(stmt, nil, tt.args.vschema, false)
			if !tt.wantErr(t, err, fmt.Sprintf("buildPlanForBypass(%v, nil, %v)", stmt, tt.args.vschema)) {
				return
			}
			got := want{
				FieldQuery: plan.primitive.(*engine.Send).FieldQuery,
			}
			assert.Equalf(t, tt.want, got, "buildPlanForBypass(%v, nil, %v)", stmt, tt.args.vschema)
		})
	}
}
