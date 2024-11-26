package wasm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateWasmFilter(t *testing.T) {
	assert.NoError(t, cluster.MysqlDb.Ping())
	assert.NoError(t, cluster.WescaleDb.Ping())
}
