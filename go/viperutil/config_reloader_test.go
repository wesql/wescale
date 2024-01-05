package viperutil

import (
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewConfigReloader(t *testing.T) {
	r := NewConfigReloader()
	fooValueIsSet := false
	barValueIsSet := false

	r.AddReloadHandler("foo", func(key string, value string, fs *pflag.FlagSet) {
		fooValueIsSet = true
		assert.Equal(t, value, "foo_value")
	})

	r.handleConfigChange("foo", "foo_value", nil)
	r.handleConfigChange("bar", "bar_value", nil)

	assert.True(t, fooValueIsSet)
	assert.False(t, barValueIsSet)
}
