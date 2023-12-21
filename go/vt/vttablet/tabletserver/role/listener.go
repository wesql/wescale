/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package role

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	mysqlRoleProbeInterval = 1 * time.Second
	mysqlRoleProbeTimeout  = 1 * time.Second
)

var (
	mysqlProbeServicePort = 3501
	mysqlProbeServiceHost = "localhost"
	getRoleUrl            = fmt.Sprintf("%s:%s/v1.0/bindings/mysql?operation=getRole", mysqlProbeServiceHost, mysqlProbeServicePort)
)

const Leader = "Leader"
const Follower = "Follower"
const Learner = "Learner"
const Logger = "Logger"

func transitionRoleType(role string) tabletpb.TabletType {
	switch role {
	case Leader:
		return tabletpb.TabletType_PRIMARY
	case Follower:
		return tabletpb.TabletType_REPLICA
	case Learner:
		return tabletpb.TabletType_RDONLY
	case Logger:
		return tabletpb.TabletType_SPARE
	default:
		return tabletpb.TabletType_UNKNOWN
	}
}

func init() {
	servenv.OnParseFor("vttablet", registerGCFlags)
}

func registerGCFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&mysqlRoleProbeInterval, "mysql_role_probe_interval", mysqlRoleProbeInterval, "Interval between garbage collection checks")
}

type Listener struct {
	isOpen          int64
	cancelOperation context.CancelFunc

	tm *tabletmanager.TabletManager

	stateMutex     sync.Mutex
	reconcileMutex sync.Mutex
}

func NewListener(tm *tabletmanager.TabletManager) *Listener {
	l := &Listener{
		isOpen: 0,
		tm:     tm,
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
				log.Info("Listener: receive tick")
				collector.reconcileLeadership(ctx)
			}
		}
	}
}

func (collector *Listener) reconcileLeadership(ctx context.Context) {
	collector.reconcileMutex.Lock()
	defer collector.reconcileMutex.Unlock()

	kvResp, err := probe(ctx, mysqlRoleProbeTimeout, http.MethodGet, getRoleUrl, nil)
	if err != nil {
		log.Errorf("try to probe mysql role, but error happened: %v", err)
		return
	}
	role, ok := kvResp["role"]
	if !ok {
		log.Errorf("unable to get mysql role from probe response, response content: %v", kvResp)
		return
	}

	// Safely assert the type of role to string.
	roleStr, ok := role.(string)
	if !ok {
		log.Error("role value is not a string, role:%v", role)
		return
	}

	tabletType := transitionRoleType(roleStr)
	switch tabletType {
	case tabletpb.TabletType_PRIMARY, tabletpb.TabletType_REPLICA, tabletpb.TabletType_RDONLY:
		collector.tm.ChangeType(context.Background(), tabletType, false)
	default:
		log.Error("role value is not a string, role:%v", role)
	}
}
