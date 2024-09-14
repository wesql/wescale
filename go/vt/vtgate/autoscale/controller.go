package autoscale

import (
	"context"
	"github.com/spf13/pflag"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

// System Config
var (
	EnableAutoScale                 = false
	AutoScaleDecisionMakingInterval = 1 * time.Second
)

// User Config
var (
	AutoSuspendTimeout = 5 * time.Minute
)

// Cluster Config
var (
	AutoScaleClusterNamespace        = "default"
	AutoScaleDataNodeStatefulSetName = "mycluster-mysql-0"
	AutoScaleDataNodePodName         = "mycluster-mysql-0-0"

	AutoScaleLoggerNodeStatefulSetName = []string{"mycluster-mysql-1", "mycluster-mysql-2"}
	AutoScaleLoggerNodePodName         = []string{"mycluster-mysql-1-0", "mycluster-mysql-2-0"}
)

func RegisterAutoScaleFlags(fs *pflag.FlagSet) {
	// System Config
	fs.BoolVar(&EnableAutoScale, "enable_auto_scale", EnableAutoScale, "enable auto scaling")
	fs.DurationVar(&AutoScaleDecisionMakingInterval, "auto_scale_decision_making_interval", AutoScaleDecisionMakingInterval, "auto scale decision making interval")
	// User Config
	fs.DurationVar(&AutoSuspendTimeout, "auto_suspend_timeout", AutoSuspendTimeout, "auto suspend timeout. default is 5m")
	// Cluster Config
	fs.StringVar(&AutoScaleClusterNamespace, "auto_scale_cluster_namespace", AutoScaleClusterNamespace, "auto scale cluster namespace")
	fs.StringVar(&AutoScaleDataNodeStatefulSetName, "auto_scale_data_node_stateful_set_name", AutoScaleDataNodeStatefulSetName, "auto scale data node stateful set name")
	fs.StringVar(&AutoScaleDataNodePodName, "auto_scale_data_node_pod_name", AutoScaleDataNodePodName, "auto scale data node pod name")
	fs.StringSliceVar(&AutoScaleLoggerNodeStatefulSetName, "auto_scale_logger_node_stateful_set_name", AutoScaleLoggerNodeStatefulSetName, "auto scale logger node stateful set name")
	fs.StringSliceVar(&AutoScaleLoggerNodePodName, "auto_scale_logger_node_pod_name", AutoScaleLoggerNodePodName, "auto scale logger node pod name")
}

func init() {
	servenv.OnParseFor("vtgate", RegisterAutoScaleFlags)
}

type AutoScaleController struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex

	svc queryservice.QueryService
}

func NewAutoScaleController(svc queryservice.QueryService) *AutoScaleController {
	w := &AutoScaleController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.svc = svc
	return w
}

func (cr *AutoScaleController) Start() {
	go func() {
		intervalTicker := time.NewTicker(AutoScaleDecisionMakingInterval)
		defer intervalTicker.Stop()

		for {
			select {
			case <-cr.ctx.Done():
				log.Infof("AutoScaleController stopped")
				return
			case <-intervalTicker.C:
			}

			// todo do something
			// 1. 获取metrics

			// 2. 判断是否需要scale up/down, scale in/out

			// 3. 执行scale up/down, scale in/out
		}
	}()
}

func (cr *AutoScaleController) Stop() {
	cr.cancelFunc()
}
