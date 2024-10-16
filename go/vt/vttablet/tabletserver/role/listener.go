/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package role

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/dbconfigs"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	mysqlRoleProbeEnable   = true
	mysqlRoleProbeInterval = 1 * time.Second
	mysqlRoleProbeTimeout  = 1 * time.Second
	// http, wesql, mysql
	mysqlRoleProbeImplementation = "wesql"
)

const (
	PRIMARY   = "primary"
	SECONDARY = "secondary"
	MASTER    = "master"
	SLAVE     = "slave"
	LEADER    = "Leader"
	FOLLOWER  = "Follower"
	LEARNER   = "Learner"
	CANDIDATE = "Candidate"
	LOGGER    = "Logger"
	UNKNOWN   = "unknown"
)

func transitionRoleType(role string) topodatapb.TabletType {
	// Convert the role to lower case for case-insensitive comparison
	role = strings.ToLower(role)

	switch role {
	case strings.ToLower(LEADER), strings.ToLower(PRIMARY), strings.ToLower(MASTER):
		return topodatapb.TabletType_PRIMARY
	case strings.ToLower(FOLLOWER), strings.ToLower(SECONDARY), strings.ToLower(SLAVE):
		return topodatapb.TabletType_REPLICA
	case strings.ToLower(CANDIDATE):
		return topodatapb.TabletType_REPLICA
	case strings.ToLower(LEARNER):
		return topodatapb.TabletType_RDONLY
	case strings.ToLower(LOGGER):
		return topodatapb.TabletType_SPARE
	default:
		log.Errorf("unknown role value: %s\n", role)
		return topodatapb.TabletType_UNKNOWN
	}
}

func init() {
	servenv.OnParseFor("vttablet", registerRoleListenerFlags)
}

func registerRoleListenerFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&mysqlRoleProbeEnable, "mysql_role_probe_enable", mysqlRoleProbeEnable, "enable mysql role probe. default is true")
	fs.DurationVar(&mysqlRoleProbeInterval, "mysql_role_probe_interval", mysqlRoleProbeInterval, "mysql role probe interval")
	fs.DurationVar(&mysqlRoleProbeTimeout, "mysql_role_probe_timeout", mysqlRoleProbeTimeout, "mysql role probe timeout")
	fs.StringVar(&mysqlRoleProbeImplementation, "mysql_role_probe_implementation", mysqlRoleProbeImplementation, "mysql role probe implementation")
}

type Listener struct {
	isOpen          int64
	cancelOperation context.CancelFunc

	stateMutex     sync.Mutex
	reconcileMutex sync.Mutex

	lastUpdate time.Time

	changeTypeFunc func(ctx context.Context, lastUpdate time.Time, targetTabletType topodatapb.TabletType) (bool, error)

	probeFunc func(ctx context.Context) (string, error)
}

func NewListener(changeTypeFunc func(ctx context.Context, lastUpdate time.Time, tabletType topodatapb.TabletType) (bool, error), dbcfgs *dbconfigs.DBConfigs) *Listener {
	var probeFunc func(ctx context.Context) (string, error)
	switch mysqlRoleProbeImplementation {
	case "http":
		probeFunc = httpProbe
	case "wesql":
		weSqlDbConfigs = dbcfgs
		probeFunc = wesqlProbe
	case "mysql":
		mysqlDbConfigs = dbcfgs
		probeFunc = mysqlProbe
	}
	l := &Listener{
		isOpen:         0,
		changeTypeFunc: changeTypeFunc,
		probeFunc:      probeFunc,
	}
	return l
}

// Open opens database pool and initializes the schema
func (collector *Listener) Open() {
	collector.stateMutex.Lock()
	defer collector.stateMutex.Unlock()
	if collector.isOpen > 0 {
		// already open
		return
	}

	log.Info("Listener: opening")
	atomic.StoreInt64(&collector.isOpen, 1)
	ctx := context.Background()
	ctx, collector.cancelOperation = context.WithCancel(ctx)
	go collector.probeLoop(ctx)
}

func (collector *Listener) Close() {
	log.Infof("Listener - started execution of Close. Acquiring initMutex lock")
	collector.stateMutex.Lock()
	defer collector.stateMutex.Unlock()
	log.Infof("Listener - acquired lock")
	if collector.isOpen == 0 {
		log.Infof("Listener - no collector is open")
		// not open
		return
	}

	log.Info("Listener: closing")
	if collector.cancelOperation != nil {
		collector.cancelOperation()
	}
	log.Infof("Listener - closing pool")
	atomic.StoreInt64(&collector.isOpen, 0)
	log.Infof("Listener - finished execution of Close")
}

func (collector *Listener) probeLoop(ctx context.Context) {
	ticker := timer.NewSuspendableTicker(mysqlRoleProbeInterval, false)
	defer ticker.Stop()
	go ticker.TickNow()

	log.Info("Listener: enter probeLoop")
	for {
		select {
		case <-ctx.Done():
			log.Info("Listener: exit probeLoop")
			return
		case <-ticker.C:
			{
				collector.reconcileLeadership(ctx)
			}
		}
	}
}

func (collector *Listener) reconcileLeadership(ctx context.Context) {
	collector.reconcileMutex.Lock()
	defer collector.reconcileMutex.Unlock()

	if !mysqlRoleProbeEnable {
		return
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, mysqlRoleProbeTimeout)
	defer cancel()

	role, err := collector.probeFunc(timeoutCtx)
	if err != nil {
		log.Errorf("failed to probe mysql role, error:%v\n", err)
		return
	}
	log.Debugf("mysqlRoleProbeImplementation: %s, probed role: %s\n", mysqlRoleProbeImplementation, role)

	tabletType := transitionRoleType(role)

	switch tabletType {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		changeTypeCtx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer cancel()
		changed, err := collector.changeTypeFunc(changeTypeCtx, collector.lastUpdate, tabletType)
		if err != nil {
			log.Errorf("change vttablet role to %s, error:%w\n", tabletType.String(), err)
		}
		if changed {
			collector.lastUpdate = time.Now()
			log.Infof("change vttablet role to %s successfully\n", tabletType.String())
		}
	default:
		log.Errorf("role value is not a string, role:%v\n", role)
	}
}
