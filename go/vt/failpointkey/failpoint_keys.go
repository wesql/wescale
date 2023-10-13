/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package failpointkey

import "github.com/pingcap/failpoint"

type FailpointKey struct {
	FullName string
	Name     string
}

var FailpointTable map[string]string

var (
	TestFailPointError = FailpointKey{
		FullName: "TestFailPointError",
		Name:     "TestFailPointError",
	}
	CreateDatabaseErrorOnDbname = FailpointKey{
		FullName: "vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname",
		Name:     "create-database-error-on-dbname",
	}
	IsVReplMigrationReadyToCutOver = FailpointKey{
		FullName: "vitess.io/vitess/go/vt/vttablet/onlineddl/IsVReplMigrationReadyToCutOver",
		Name:     "IsVReplMigrationReadyToCutOver",
	}
)

func init() {
	err := failpoint.Enable("vitess.io/vitess/go/vt/vtgate/OpenSetFailPoint", "return(1)")
	if err != nil {
		return
	}
	FailpointTable = make(map[string]string)
	FailpointTable[CreateDatabaseErrorOnDbname.FullName] = CreateDatabaseErrorOnDbname.Name
	FailpointTable[TestFailPointError.FullName] = TestFailPointError.Name
	FailpointTable[IsVReplMigrationReadyToCutOver.FullName] = IsVReplMigrationReadyToCutOver.Name
}
