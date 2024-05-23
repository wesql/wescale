package main

import "github.com/wesql/wescale/go/stats/statsd"

func init() {
	statsd.Init("vtgate")
}
