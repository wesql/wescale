/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wesql/wescale/go/mysql"
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
