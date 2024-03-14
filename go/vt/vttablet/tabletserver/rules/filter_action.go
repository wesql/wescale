/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package rules

type FilterAction struct {
	// Action is the action to take if the rule matches
	Action Action

	Rule *Rule
}

// todo earayu: replace me with a better solution
var EmptyFilterAction = &FilterAction{
	Action: QRContinue,
	Rule:   NewQueryRule("empty", "empty", QRContinue),
}
