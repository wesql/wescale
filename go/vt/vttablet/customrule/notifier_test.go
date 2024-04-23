package customrule

import "testing"

func TestNotifyReload_ChannelNotFull(t *testing.T) {
	NotifyReload()
	select {
	case <-customRuleChanged:
		// Pass
	default:
		t.Fail()
	}
}

func TestNotifyReload_ChannelFull(_ *testing.T) {
	customRuleChanged <- struct{}{}
	NotifyReload() // Should not block
}

func TestWatch(t *testing.T) {
	ch := Watch()
	if ch != customRuleChanged {
		t.Fail()
	}
}
