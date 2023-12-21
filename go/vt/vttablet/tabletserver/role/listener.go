/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package role

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

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
	mysqlProbeServicePort int64 = 3501
	mysqlProbeServiceHost       = "localhost"
)

const LORRY_HTTP_PORT_ENV_NAME = "LORRY_HTTP_PORT"

const Leader = "Leader"
const Follower = "Follower"
const Learner = "Learner"
const Logger = "Logger"

func transitionRoleType(role string) topodatapb.TabletType {
	switch role {
	case Leader:
		return topodatapb.TabletType_PRIMARY
	case Follower:
		return topodatapb.TabletType_REPLICA
	case Learner:
		return topodatapb.TabletType_RDONLY
	case Logger:
		return topodatapb.TabletType_SPARE
	default:
		return topodatapb.TabletType_UNKNOWN
	}
}

func init() {
	servenv.OnParseFor("vttablet", registerGCFlags)
}

func setUpMysqlProbeServicePort() {
	portStr, ok := os.LookupEnv(LORRY_HTTP_PORT_ENV_NAME)
	if !ok {
		return
	}
	// parse portStr to int
	portFromEnv, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return
	}
	mysqlProbeServicePort = portFromEnv
}

func registerGCFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&mysqlRoleProbeInterval, "mysql_role_probe_interval", mysqlRoleProbeInterval, "Interval between garbage collection checks")
	fs.DurationVar(&mysqlRoleProbeTimeout, "mysql_role_probe_timeout", mysqlRoleProbeTimeout, "Interval between garbage collection checks")

	fs.StringVar(&mysqlProbeServiceHost, "mysql_role_probe_timeout", mysqlProbeServiceHost, "Interval between garbage collection checks")
}

type Listener struct {
	isOpen          int64
	cancelOperation context.CancelFunc

	stateMutex     sync.Mutex
	reconcileMutex sync.Mutex

	changeTypeFunc func(ctx context.Context, tabletType topodatapb.TabletType)
}

func NewListener(changeTypeFunc func(ctx context.Context, tabletType topodatapb.TabletType)) *Listener {
	l := &Listener{
		isOpen:         0,
		changeTypeFunc: changeTypeFunc,
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

	setUpMysqlProbeServicePort()
	getRoleUrl := fmt.Sprintf("http://%s:%d/v1.0/bindings/mysql?operation=getRole", mysqlProbeServiceHost, mysqlProbeServicePort)

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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		changeTypeCtx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer cancel()
		collector.changeTypeFunc(changeTypeCtx, tabletType)
	default:
		log.Error("role value is not a string, role:%v", role)
	}
}
