package main

import (
	"log"

	"github.com/wesql/wescale/go/acl"
	"github.com/wesql/wescale/go/cmd/rulesctl/cmd"
	vtlog "github.com/wesql/wescale/go/vt/log"
	"github.com/wesql/wescale/go/vt/logutil"
	"github.com/wesql/wescale/go/vt/servenv"
)

func main() {
	rootCmd := cmd.Main()
	vtlog.RegisterFlags(rootCmd.PersistentFlags())
	logutil.RegisterFlags(rootCmd.PersistentFlags())
	acl.RegisterFlags(rootCmd.PersistentFlags())
	servenv.RegisterMySQLServerFlags(rootCmd.PersistentFlags())
	if err := rootCmd.Execute(); err != nil {
		log.Printf("%v", err)
	}
}
