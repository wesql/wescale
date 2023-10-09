/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/topodata"
)

func Test_suggestTabletType_to_replica(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		readWriteSplittingRatio  int32
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				readWriteSplittingRatio:  int32(100),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				readWriteSplittingRatio:  int32(100),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.readWriteSplittingRatio, tt.args.sql, false, false)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_to_primary(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		readWriteSplittingRatio  int32
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "disable",
				readWriteSplittingRatio:  int32(100),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "disable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "INSERT INTO users (id, name) VALUES (1, 'foo');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "UPDATE users SET name = 'foo' WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "DELETE FROM users WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.readWriteSplittingRatio, tt.args.sql, false, false)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_force_primary(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		readWriteSplittingRatio  int32
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				readWriteSplittingRatio:  int32(100),
				inTransaction:            true,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=true, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     true,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=true",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          true,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT last_insert_id();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users lock in share mode;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users for update;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT get_lock('lock', 10);",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT release_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT is_used_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT is_free_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT release_all_locks();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from performance_schema.metadata_locks;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from information_schema.innodb_trx;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from mysql.user",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from sys.sys_config;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.readWriteSplittingRatio, tt.args.sql, false, false)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_random(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		readWriteSplittingRatio  int32
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantRatio      float32
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false, ratio=70",
			args: args{
				readWriteSplittingPolicy: "random",
				readWriteSplittingRatio:  int32(70),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantRatio: 0.7,
			wantErr:   assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false, ratio=70",
			args: args{
				readWriteSplittingPolicy: "disable",
				readWriteSplittingRatio:  int32(70),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantRatio: 0,
			wantErr:   assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false. ratio=100",
			args: args{
				readWriteSplittingPolicy: "random",
				readWriteSplittingRatio:  int32(100),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantRatio: 1,
			wantErr:   assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false. ratio=0",
			args: args{
				readWriteSplittingPolicy: "random",
				readWriteSplittingRatio:  int32(0),
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantRatio: 0,
			wantErr:   assert.NoError,
		},
	}

	primaryTypeCount, replicaTypeCount := 0, 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				gotTabletType, _ := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.readWriteSplittingRatio, tt.args.sql, false, false)
				switch gotTabletType {
				case topodata.TabletType_PRIMARY:
					primaryTypeCount++
				case topodata.TabletType_REPLICA:
					replicaTypeCount++
				}
			}
		})
		assert.Equalf(t, primaryTypeCount+replicaTypeCount, 1000, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		ratio := float32(replicaTypeCount) / float32(replicaTypeCount+primaryTypeCount)
		assert.LessOrEqualf(t, math.Abs(float64(ratio-tt.wantRatio)), 0.1, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		primaryTypeCount, replicaTypeCount = 0, 0
	}
}

func Test_suggestTabletType_read_only_transaction(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy           string
		EnableReadWriteSplitForReadOnlyTxn bool
		readWriteSplittingRatio            int32
		inTransaction                      bool
		hasCreatedTempTables               bool
		hasAdvisoryLock                    bool
		sql                                string
		isInReadOnlyTx                     bool
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantRatio      float32
		wantErr        assert.ErrorAssertionFunc
	}{
		// the value of inTransaction in the following test excepts the last one will be true
		// when readWriteSplitting is disabled and read only transaction is disabled, tablet type should be primary
		{
			name: "readWriteSplittingPolicy=disable, EnableReadWriteSplitForReadOnlyTxn=false, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false, ratio=70",
			args: args{
				readWriteSplittingPolicy:           "disable",
				EnableReadWriteSplitForReadOnlyTxn: false,
				readWriteSplittingRatio:            int32(70),
				inTransaction:                      true,
				hasCreatedTempTables:               false,
				hasAdvisoryLock:                    false,
				sql:                                "SELECT * FROM users;",
				isInReadOnlyTx:                     true,
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantRatio:      0,
			wantErr:        assert.NoError,
		},
		// when readWriteSplitting is disabled and read only transaction is enabled, tablet type should be primary
		{
			name: "readWriteSplittingPolicy=disable, EnableReadWriteSplitForReadOnlyTxn=true, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false, ratio=70",
			args: args{
				readWriteSplittingPolicy:           "disable",
				EnableReadWriteSplitForReadOnlyTxn: true,
				readWriteSplittingRatio:            int32(70),
				inTransaction:                      true,
				hasCreatedTempTables:               false,
				hasAdvisoryLock:                    false,
				sql:                                "SELECT * FROM users;",
				isInReadOnlyTx:                     true,
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantRatio:      0,
			wantErr:        assert.NoError,
		},
		// when readWriteSplitting is random and read only transaction is disabled, the result ratio should be close to ratio wanted
		{
			name: "readWriteSplittingPolicy=enable, EnableReadWriteSplitForReadOnlyTxn=false, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false. ratio=70",
			args: args{
				readWriteSplittingPolicy:           "random",
				EnableReadWriteSplitForReadOnlyTxn: false,
				readWriteSplittingRatio:            int32(70),
				inTransaction:                      true,
				hasCreatedTempTables:               false,
				hasAdvisoryLock:                    false,
				sql:                                "SELECT * FROM users;",
				isInReadOnlyTx:                     true,
			},
			wantRatio: 0, // the sql is about read, but it is in a tx, and read only tx is disabled, so will route to
			wantErr:   assert.NoError,
		},
		// when readWriteSplitting is random and read only transaction is enabled, all sql should be routed to REPLICATE
		{
			name: "readWriteSplittingPolicy=enable, EnableReadWriteSplitForReadOnlyTxn=true, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false. ratio=0",
			args: args{
				readWriteSplittingPolicy:           "random",
				EnableReadWriteSplitForReadOnlyTxn: true,
				readWriteSplittingRatio:            int32(0),
				inTransaction:                      true,
				hasCreatedTempTables:               false,
				hasAdvisoryLock:                    false,
				sql:                                "SELECT * FROM users;",
				isInReadOnlyTx:                     true,
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantRatio:      1, // read only tx is enabled, so all read only tx will be routed to read only tablet
			wantErr:        assert.NoError,
		},
		// when readWriteSplitting is random and read only transaction is enabled, the sql "start transaction read only" should be routed to REPLICATE
		{
			name: "readWriteSplittingPolicy=enable, EnableReadWriteSplitForReadOnlyTxn=true, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false. ratio=0",
			args: args{
				readWriteSplittingPolicy:           "random",
				EnableReadWriteSplitForReadOnlyTxn: true,
				readWriteSplittingRatio:            int32(70),
				inTransaction:                      false,
				hasCreatedTempTables:               false,
				hasAdvisoryLock:                    false,
				sql:                                "START TRANSACTION READ ONLY;",
				isInReadOnlyTx:                     false,
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantRatio:      0, // read only tx begin sql will not route to mysql to execute actually, and is not a pure select sql, so the ratio is 0
			wantErr:        assert.NoError,
		},
	}

	primaryTypeCount, replicaTypeCount := 0, 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				gotTabletType, _ := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.readWriteSplittingRatio, tt.args.sql, tt.args.EnableReadWriteSplitForReadOnlyTxn, tt.args.isInReadOnlyTx)
				switch gotTabletType {
				case topodata.TabletType_PRIMARY:
					primaryTypeCount++
				case topodata.TabletType_REPLICA:
					replicaTypeCount++
				}
			}
		})
		assert.Equalf(t, primaryTypeCount+replicaTypeCount, 1000, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		ratio := float32(replicaTypeCount) / float32(replicaTypeCount+primaryTypeCount)
		fmt.Printf("ratio is %f", ratio)
		assert.LessOrEqualf(t, math.Abs(float64(ratio-tt.wantRatio)), 0.1, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		primaryTypeCount, replicaTypeCount = 0, 0
	}
}
