/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package mysqlbasedacl

import (
	"testing"

	"vitess.io/vitess/go/vt/tableacl/testlib"
)

func TestMysqlbasedACL(t *testing.T) {
	testlib.TestSuiteMysqlBaseACL(t, &Factory{})
}
