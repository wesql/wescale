package customrule

var customRuleChanged = make(chan struct{}, 1)

func NotifyReload() {
	select {
	case customRuleChanged <- struct{}{}:
	default:
	}
}

func Watch() <-chan struct{} {
	return customRuleChanged
}

var WaitForFilter func(name string, shouldExists bool) error
