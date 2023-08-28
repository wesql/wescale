package vtgate

import (
	"github.com/stretchr/testify/assert"
	"testing"
)
import "github.com/pingcap/failpoint"

func GetValueOfFailPoint(fpName string) any {
	failpoint.Inject(fpName, func(v failpoint.Value) {
		failpoint.Return(v)
	})
	return nil
}

func TestFailpointEnable(t *testing.T) {
	failpoint.Enable("vitess.io/vitess/go/vt/vtgate/testPanic", "return(1)")

	assert.Equal(t, 1, GetValueOfFailPoint("testPanic"))
}
