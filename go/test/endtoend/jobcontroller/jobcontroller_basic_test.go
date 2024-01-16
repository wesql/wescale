/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

import (
	"testing"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestA(t *testing.T) {
	t.Log("TestA")
}

func TestB(t *testing.T) {
	t.Log("TestA")
}
