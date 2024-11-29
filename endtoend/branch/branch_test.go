package wasm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSourceAndTargetClusterConnection(t *testing.T) {
	assert.NoError(t, sourceCluster.MysqlDb.Ping())
	assert.NoError(t, sourceCluster.WescaleDb.Ping())
	assert.NoError(t, targetCluster.MysqlDb.Ping())
	assert.NoError(t, targetCluster.WescaleDb.Ping())
}
