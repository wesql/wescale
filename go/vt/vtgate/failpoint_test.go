/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/failpoint"
)

func GetValueOfFailPoint(fpName string) failpoint.Value {
	failpoint.Inject(fpName, func(v failpoint.Value) {
		failpoint.Return(v)
	})
	return 0
}

func TestFailpointEnable(t *testing.T) {
	failpoint.Enable("vitess.io/vitess/go/vt/vtgate/testPanic", "return(1)")
	require.Equal(t, 1, GetValueOfFailPoint("testPanic"))
}
