/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestNewLastSeenGtid(t *testing.T) {
	gs, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)

	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1")
	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:2")
	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:3")

	assert.Equal(t, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-3", gs.String())

	assert.Equal(t, gs.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-3"))
}

func GtidSetRangeAddGtid(gtidSet *LastSeenGtid, start int64, end int64, sid string) {
	for i := start; i <= end; i++ {
		err := (*gtidSet).AddGtid(fmt.Sprintf("%s:%d", sid, i))
		if err != nil {
			return
		}
	}
}

func TestLastSeenGtid_MergeGtidSets(t *testing.T) {
	gs1, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	GtidSetRangeAddGtid(gs1, 1, 100, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")
	GtidSetRangeAddGtid(gs1, 102, 103, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")
	require.NoError(t, err)
	gs2, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)
	GtidSetRangeAddGtid(gs2, 1, 101, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")

	gs1.MergeGtidSets(&gs2.gtidSet)
	assert.Equal(t, gs1.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-103"))
}
func TestLastSeenGtid_MergeGtidSets2(t *testing.T) {
	gs1, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:10")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:20")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:30")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:50")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:300")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:500")

	require.NoError(t, err)
	gs2, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)
	GtidSetRangeAddGtid(gs2, 1, 300, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")

	gs1.MergeGtidSets(&gs2.gtidSet)
	assert.Equal(t, gs1.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-300:500"))
}
func TestLastSeenGtid_MergeGtidSets3(t *testing.T) {
	gs1, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:3")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:5")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:7")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:10")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:15")

	require.NoError(t, err)
	gs2, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)
	GtidSetRangeAddGtid(gs2, 1, 100, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")

	gs1.MergeGtidSets(&gs2.gtidSet)
	assert.Equal(t, gs1.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-100"))
}
func TestLastSeenGtid_MergeGtidSets4(t *testing.T) {
	gs1, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:3")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:5")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:7")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:10")
	gs1.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:15")

	require.NoError(t, err)
	gs2, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)
	GtidSetRangeAddGtid(gs2, 1, 6, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")
	GtidSetRangeAddGtid(gs2, 11, 14, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5")

	gs1.MergeGtidSets(&gs2.gtidSet)
	assert.Equal(t, gs1.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-7:10-15"))
}

func TestNewLastSeenGtid_MultiServer(t *testing.T) {
	gs, err := NewLastSeenGtid(mysql.Mysql56FlavorID)
	require.NoError(t, err)

	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1")
	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:2")
	gs.AddGtid("df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:3")
	gs.AddGtid("ddfabe04-d9b4-11ed-8345-d22027637c46:1")

	assert.Equal(t, "ddfabe04-d9b4-11ed-8345-d22027637c46:1,df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-3", gs.String())

	assert.Equal(t, gs.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "ddfabe04-d9b4-11ed-8345-d22027637c46:1,    df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-3"))
	assert.Equal(t, gs.Position(), mysql.MustParsePosition(mysql.Mysql56FlavorID, "df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-3,ddfabe04-d9b4-11ed-8345-d22027637c46:1"))

	assert.True(t, mysql.MustParsePosition(mysql.Mysql56FlavorID,
		"df74afe2-d9b4-11ed-b2c8-f8b7ac3813b5:1-135147,\ne0ebc27a-d9b4-11ed-a01c-07ef9a363f9b:1,ddfabe04-d9b4-11ed-8345-d22027637c46:1").AtLeast(gs.Position()))
}
