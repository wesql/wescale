/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"context"
	"encoding/json"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

const (
	InternalInfoKeyEcho                     = "echo"
	InternalInfoKeyTabletInfo               = "tablet_info_.*"
	InternalInfoKeygetFromGlobalTopoServer_ = "getFromGlobalTopoServer_.*"
)

func GetInternalInfo(key string, e *Executor) (string, error) {
	if key == InternalInfoKeyEcho {
		return key, nil
	}
	if inst.RegexpMatchPatterns(key, []string{InternalInfoKeyTabletInfo}) {
		return GetTabletInfo(key, e)
	}
	if inst.RegexpMatchPatterns(key, []string{InternalInfoKeygetFromGlobalTopoServer_}) {
		return GetFromTopoServer(key, e)
	}
	return "", nil
}

// GetTabletInfo returns the tablet info for the given tablet alias.
// Usage:
// set @internal_info_key='tablet_info_zone1-100';
// select internal_info();
func GetTabletInfo(key string, e *Executor) (string, error) {
	topoServer, err := e.serv.GetTopoServer()
	if err != nil {
		return "", err
	}
	alias, err := topoproto.ParseTabletAlias(key[len("tablet_info_"):])
	if err != nil {
		return "", err
	}
	tabletInfo, err := topoServer.GetTablet(context.Background(), alias)

	if err != nil {
		return "", err
	}
	j, err := json.MarshalIndent(tabletInfo, "", "\t")
	if err != nil {
		return "", err
	}
	return string(j), nil
}

func GetFromTopoServer(key string, e *Executor) (string, error) {
	topoServer, err := e.serv.GetTopoServer()
	if err != nil {
		return "", err
	}
	conn, err := topoServer.ConnForCell(context.Background(), topo.GlobalCell)
	if err != nil {
		return "", err
	}
	filePath := key[len("getFromGlobalTopoServer_"):]
	data, _, err := conn.Get(context.Background(), filePath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
