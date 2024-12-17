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

var WaitForFilterLoad func(name string) error
var WaitForFilterDelete func(name string) error
