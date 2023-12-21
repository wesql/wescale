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

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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

func init() {
	servenv.OnParseFor("vttablet", registerGCFlags)
}

func registerGCFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&mysqlRoleProbeInterval, "mysql_role_probe_interval", mysqlRoleProbeInterval, "Interval between garbage collection checks")
}

type Listener struct {
	isOpen          int64
	cancelOperation context.CancelFunc

	env tabletenv.Env
	ts  *topo.Server

	stateMutex sync.Mutex
}

func NewListener(env tabletenv.Env, ts *topo.Server) *Listener {
	l := &Listener{
		isOpen: 0,
		env:    env,
		ts:     ts,
	}
	return l
}

// Open opens database pool and initializes the schema
func (collector *Listener) Open() (err error) {
	collector.stateMutex.Lock()
	defer collector.stateMutex.Unlock()
	if collector.isOpen > 0 {
		// already open
		return nil
	}
	if !collector.env.Config().EnableTableGC {
		return nil
	}

	if err != nil {
		return fmt.Errorf("Error parsing --table_gc_lifecycle flag: %+v", err)
	}

	log.Info("Listener: opening")
	atomic.StoreInt64(&collector.isOpen, 1)
	ctx := context.Background()
	ctx, collector.cancelOperation = context.WithCancel(ctx)
	go collector.probeLoop(ctx)

	return nil
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
				checkLeadership(ctx)
			}
		}
	}
}

func checkLeadership(ctx context.Context) {
	kvResp, err := probe(ctx, http.MethodGet, getRoleUrl, nil)
	if err != nil {

	}
	role := kvResp["role"].(string)
	if role == "Leader" {

	}
}

// RoleResponse 是我们期望从HTTP响应中解析出的JSON结构
type RoleResponse struct {
	Event string `json:"event"`
	Role  string `json:"role"`
}
