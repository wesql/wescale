/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package failpointutil

var FailpointTable map[int]string

const (
	TestFailPoint = iota
)

var (
	TestFailPointStr = "testFailPoint"
)

func init() {
	FailpointTable = make(map[int]string)
	FailpointTable[TestFailPoint] = TestFailPointStr
}
