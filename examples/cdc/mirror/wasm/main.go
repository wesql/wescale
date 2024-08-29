//go:build wasip1

package main

import (
	"context"
	"github.com/stealthrocket/net/wasip1"
	cdc "github.com/wesql/wescale-cdc"
	"net"
)

func main() {
	cc := cdc.NewCdcConsumer()
	cc.DialContextFunc = func(ctx context.Context, address string) (net.Conn, error) {
		return wasip1.DialContext(ctx, "tcp", address)
	}
	cc.Open()
	defer cc.Close()

	cc.Run()
}
