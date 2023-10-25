/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package vtgate

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	InternalInfoKeyEcho       = "echo"
	InternalInfoKeyTabletInfo = "tablet_info"
)

func GetInternalInfo(key string, e *Executor) (string, error) {
	switch key {
	case InternalInfoKeyEcho:
		return key, nil
	case InternalInfoKeyTabletInfo:
		return GetTabletInfo(e)
	default:
		return "", nil
	}
}

func GetTabletInfo(e *Executor) (string, error) {
	topoServer, err := e.serv.GetTopoServer()
	if err != nil {
		return "", err
	}
	tabletInfo, err := topoServer.GetTablet(context.Background(), &topodatapb.TabletAlias{
		Cell: e.cell,
		Uid:  uint32(100),
	})
	if err != nil {
		return "", err
	}
	return tabletInfo.String(), nil
}
