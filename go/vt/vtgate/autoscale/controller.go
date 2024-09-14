package autoscale

import (
	"context"
	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

// System Config
var (
	EnableAutoScale                 = false
	AutoScaleDecisionMakingInterval = 5 * time.Second
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

	svc    queryservice.QueryService
	config *rest.Config
}

func NewAutoScaleController(svc queryservice.QueryService) *AutoScaleController {
	log.Infof("NewAutoScaleController")
	w := &AutoScaleController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.svc = svc
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Errorf("Error building in-cluster config: %s", err.Error())
	}
	w.config = config
	log.Infof("AutoScaleController config: %v", w.config)
	return w
}

func (cr *AutoScaleController) Start() {
	log.Infof("AutoScaleController started")
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

			if cr.config == nil {
				log.Errorf("scale controller config is nil")
				continue
			}

			// todo do something
			// 1. 获取metrics
			err := TrackCPUAndMemory(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("track cpu and memory error: %v", err)
				continue
			}
			totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit, err := GetRequestAndLimitMetrics(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("get request and limit metrics error: %v", err)
				continue
			}
			log.Infof("totalCPURequest: %v, totalMemoryRequest: %v, totalCPULimit: %v, totalMemoryLimit :%v\n",
				totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit)

			// 2. 判断是否需要scale up/down, scale in/out
			// todo scale in
			cpuHistory, memoryHistory := GetCPUAndMemoryHistory()
			log.Infof("cpuHistory: %v, memoryHistory: %v\n", cpuHistory, memoryHistory)

			e := NaiveEstimator{CPUUpperMargin: 500,
				CPULowerMargin:    500,
				MemoryUpperMargin: 500,
				MemoryLowerMargin: 500,
				CPUDelta:          500,
				MemoryDelta:       500}
			cpuUpper, cpuLower, memoryUpper, memoryLower := e.Estimate(cpuHistory, totalCPULimit, totalCPURequest, 4000, 500,
				memoryHistory, totalMemoryLimit, totalMemoryRequest, 5000, 500)
			log.Infof("cpuUpper: %v, cpuLower: %v,"+
				"memoryUpper: %v, memoryLower:%v \n", cpuUpper, cpuLower, memoryUpper, memoryLower)

			// 3. 执行scale up/down, scale in/out
			clientset, err := kubernetes.NewForConfig(cr.config)
			if err != nil {
				log.Errorf("Error creating clientset: %s", err.Error())
				continue
			}
			err = scaleUpDownStatefulSet(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName, cpuLower, totalMemoryRequest, cpuUpper, totalMemoryLimit)
			if err != nil {
				log.Errorf("scale up/down stateful set error: %v", err)
			}
			log.Infof("scale up and down successfully")
		}
	}()
}

func (cr *AutoScaleController) Stop() {
	cr.cancelFunc()
}
