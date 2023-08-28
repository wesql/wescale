/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/pingcap/failpoint"
)

func GetValueOfFailPoint(fpName string) int {
	failpoint.Inject(fpName, func(v failpoint.Value) {
		failpoint.Return(v)
	})
	return 0
}

func TestFailpointEnable(t *testing.T) {
	//failpoint.Enable("vitess.io/vitess/go/vt/vtgate/testPanic", "return(1)")

	fmt.Println("testPanic", GetValueOfFailPoint("testPanic"))
	assert.Equal(t, 0, GetValueOfFailPoint("testPanic"))
}
