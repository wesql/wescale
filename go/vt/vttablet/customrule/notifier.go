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
