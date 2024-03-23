/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package tabletserver

import "vitess.io/vitess/go/vt/vttablet/tabletserver/rules"

const defaultPriority = 1000

type PluginInterface interface {
	BeforeExecution(qre *QueryExecutor) error

	GetRule() *rules.Rule
}
