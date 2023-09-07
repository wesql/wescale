/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package failpointkey

type FailpointKey struct {
	FullName string
	Name     string
}

var FailpointTable map[string]string

var (
	CreateDatabaseErrorOnDbname = FailpointKey{
		FullName: "vitess.io/vitess/go/vt/topotools/create-database-error-on-dbname",
		Name:     "create-database-error-on-dbname",
	}
)

func init() {
	FailpointTable = make(map[string]string)
	FailpointTable[CreateDatabaseErrorOnDbname.FullName] = CreateDatabaseErrorOnDbname.Name
}
