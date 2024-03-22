/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package plugin

const defaultPriority = 1000

type PluginInterface interface {
	BeforeExecution() error

	AfterExecution() error

	//todo earayu: remove this method? Plugin should not know about priority
	GetPriority() int
}
