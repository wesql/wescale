/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVTConsensusLogger(t *testing.T) {
	logger := NewVTConsensusLogger("test_ks", "0")
	s1 := logger.annotate("apecloud")
	assert.Equal(t, "shard=test_ks/0 apecloud", s1)
	s2 := fmt.Sprintf(logger.annotate("apecloud %s"), "def")
	assert.Equal(t, "shard=test_ks/0 apecloud def", s2)
}
