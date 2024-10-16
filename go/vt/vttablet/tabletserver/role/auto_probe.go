package role

import (
	"context"
	"fmt"
	"vitess.io/vitess/go/vt/log"

	"github.com/spf13/pflag"
	"vitess.io/vitess/go/vt/servenv"
)

type probeFuncEntry struct {
	name string
	fn   ProbeFunc
}

func (p *probeFuncEntry) String() string {
	return p.name
}

var (
	autoRoleProbeImplementationList = []string{"http", "wesql", "mysql"}
	currentProbeFuncEntryList       = make([]probeFuncEntry, 0)
)

func init() {
	servenv.OnParseFor("vttablet", registerAutoProbeFlags)
}

func registerAutoProbeFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&autoRoleProbeImplementationList, "auto_role_probe_implementation_list", autoRoleProbeImplementationList, "List of probe implementations to determine the role")
}

func reloadAutoProbeFuncList() {
	currentProbeFuncEntryList = make([]probeFuncEntry, 0)
	log.Infof("Reloading auto probe function list: %s", autoRoleProbeImplementationList)
	for _, probeFuncName := range autoRoleProbeImplementationList {
		probeFunc, ok := ProbeFuncMap[probeFuncName]
		if !ok {
			log.Errorf("Probe function '%s' not found", probeFuncName)
			continue
		}
		currentProbeFuncEntryList = append(currentProbeFuncEntryList, probeFuncEntry{name: probeFuncName, fn: probeFunc})
	}
}

func autoProbe(ctx context.Context) (string, error) {
	if len(autoRoleProbeImplementationList) == 0 {
		return "", fmt.Errorf("no probe implementations specified")
	}
	if len(currentProbeFuncEntryList) == 0 {
		// Reload the probe function list if it is empty, this may happen if the probe function all failed at least once
		reloadAutoProbeFuncList()
	}

	log.Debugf("Current probe function list: %v", currentProbeFuncEntryList)

	var lastErr error
	// use probeFunc in currentProbeFuncEntryList to probe the role, if failed, remove it from the list
	for len(currentProbeFuncEntryList) > 0 {
		probeFuncEntry := currentProbeFuncEntryList[0]
		probeFuncName := probeFuncEntry.name
		probeFunc := probeFuncEntry.fn
		role, err := probeFunc(ctx)
		if err != nil {
			log.Errorf("Probe function '%s' failed: %v, remove it", probeFuncName, err)
			currentProbeFuncEntryList = currentProbeFuncEntryList[1:]
			lastErr = err
			continue
		}
		return role, nil
	}
	return UNKNOWN, fmt.Errorf("all probe functions failed, last error: %v", lastErr)
}
