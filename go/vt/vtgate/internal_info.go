/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"context"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

const (
	InternalInfoKeyEcho       = "echo"
	InternalInfoKeyTabletInfo = "tablet_info_.*"
)

func GetInternalInfo(key string, e *Executor) (string, error) {
	if key == InternalInfoKeyEcho {
		return key, nil
	}
	if inst.RegexpMatchPatterns(key, []string{InternalInfoKeyTabletInfo}) {
		return GetTabletInfo(key, e)
	}
	return "", nil
}

/**
 * Usage:
 * set @internal_info_key='tablet_info_zone1-100';
 * select internal_info();
 */
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
	return tabletInfo.String(), nil
}
