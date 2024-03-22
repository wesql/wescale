/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package plugin

import (
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

const defaultPriority = 1000

type PluginInterface interface {
	BeforeExecution(qre *tabletserver.QueryExecutor) error

	GetRule() *rules.Rule
}
