//go:build wasip1

package main

import (
	"context"
	"github.com/stealthrocket/net/wasip1"
	cdc "github.com/wesql/wescale-cdc"
	_ "github.com/wesql/wescale/examples/cdc/vector"
	"net"
)

func main() {
	cdc.SpiInfof("cdc consumer:[vector] is running\n")

	cc := cdc.NewCdcConsumer()
	cc.DialContextFunc = func(ctx context.Context, address string) (net.Conn, error) {
		return wasip1.DialContext(ctx, "tcp", address)
	}
	cc.Open()
	defer cc.Close()

	cc.Run()
}
