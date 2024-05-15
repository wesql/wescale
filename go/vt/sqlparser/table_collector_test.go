package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectTables(t *testing.T) {
	tests := []struct {
		query              string
		defaultTableSchema string
		want               []TableSchemaAndName
	}{
		{
			query:              "select * from t",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "select * from t1 union select * from t2",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "insert into t values()",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "update t set a=1",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "delete from t",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "set a=1",
			defaultTableSchema: "db",
			want:               nil,
		}, {
			query:              "show variable like 'a%'",
			defaultTableSchema: "db",
			want:               nil,
		}, {
			query:              "describe select * from t",
			defaultTableSchema: "db",
			want:               nil,
		}, {
			query:              "create table t",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "rename table t1 to t2",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "flush tables t1, t2",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "drop table t",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t",
			}},
		}, {
			query:              "repair t",
			defaultTableSchema: "db",
			want:               nil,
		}, {
			query:              "select (select a from t2) from t1",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "insert into t1 values((select a from t2), 1)",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "update t1 set a = (select b from t2)",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "delete from t1 where a = (select b from t2)",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "select * from t1, t2",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "select * from (t1, t2)",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "update t1 join t2 on a=b set c=d",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "update (select * from t1) as a join t2 on a=b set c=d",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "t1",
			}, {
				schema: "db",
				name:   "t2",
			}},
		}, {
			query:              "INSERT INTO study_subject ( NAME, parent_id, ancestors, del_flag ) VALUES ( '测试', 0, IF ( TRUE, 0, ( SELECT concat( t1.ancestors, ',', 0 ) FROM study_subject t1 WHERE t1.id = 0 )), '0')",
			defaultTableSchema: "db",
			want: []TableSchemaAndName{{
				schema: "db",
				name:   "study_subject",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := Parse(tt.query)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equalf(t, tt.want, CollectTables(stmt, tt.defaultTableSchema), "CollectTables(%v, %v)", stmt, tt.defaultTableSchema)
		})
	}
}
