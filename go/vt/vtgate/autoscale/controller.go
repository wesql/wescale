package autoscale

import (
	"context"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

//global todo: need to validate all variables

// System Config
var (
	EnableAutoScale                 = true
	EnableAutoSuspend               = true
	AutoScaleDecisionMakingInterval = 5 * time.Second
)

// User Config
var (
	AutoSuspendQpsSampleInterval = 10 * time.Second //todo need to larger than 1s, otherwise vtgate will panic
	AutoSuspendTimeout           = 5 * time.Minute  //todo must larger than AutoSuspendQpsSampleInterval
)

const (
	Gi = 1024 * 1024 * 1024
	Mi = 1024 * 1024
	Ki = 1024
)

// Cluster Config
var (
	//todo All these variables should be read from the environment
	AutoScaleClusterNamespace        = "default"
	AutoScaleDataNodeStatefulSetName = "mycluster-wesql-0"
	AutoScaleDataNodePodName         = "mycluster-wesql-0-0"

	AutoScaleLoggerNodeStatefulSetName = []string{"mycluster-wesql-1", "mycluster-wesql-2"}
	AutoScaleLoggerNodePodName         = []string{"mycluster-wesql-1-0", "mycluster-wesql-2-0"}
)

func RegisterAutoScaleFlags(fs *pflag.FlagSet) {
	// System Config
	fs.BoolVar(&EnableAutoSuspend, "enable_auto_suspend", EnableAutoSuspend, "enable auto suspend")
	fs.DurationVar(&AutoScaleDecisionMakingInterval, "auto_scale_decision_making_interval", AutoScaleDecisionMakingInterval, "auto scale decision making interval")
	fs.BoolVar(&EnableAutoScale, "enable_auto_scale", EnableAutoScale, "enable auto scaling up and down")

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
	servenv.OnParseFor("vtgate", RegisterAutoScaleEstimatorFlags)
}

// The number of replicas of the DataNode's StatefulSet that we want to have
var ExpectedDataNodeStatefulSetReplicas int32 = 1

// The number of replicas of the DataNode's StatefulSet that currently exist
var CurrentDataNodeStatefulSetReplicas int32 = 1

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
			case <-WatchQPSHistoryChange():
				QPSByDbType.Snapshot()
			}

			if cr.config == nil {
				log.Errorf("scale controller config is nil")
				continue
			}
			clientset, err := kubernetes.NewForConfig(cr.config)
			if err != nil {
				log.Errorf("Error creating clientset: %s", err.Error())
				continue
			}

			// Extracted code into doAutoSuspend function
			cr.doAutoSuspend(clientset)

			// 2. Collect CPU and memory metrics to determine if scaling up/down is needed
			if ExpectedDataNodeStatefulSetReplicas == 0 || !EnableAutoScale {
				// No need to scale up or down
				continue
			}

			err = TrackCPUAndMemory(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("track cpu and memory error: %v", err)
				continue
			}
			currentCPURequest, currentMemoryRequest, currentCPULimit, currentMemoryLimit, err := GetRequestAndLimitMetrics(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("get request and limit metrics error: %v", err)
				continue
			}
			log.Infof("currentCPURequest: %v mCore, currentMemoryRequest: %v bytes, currentCPULimit: %v mCore, currentMemoryLimit :%v bytes\n",
				currentCPURequest, currentMemoryRequest, currentCPULimit, currentMemoryLimit)

			cpuHistory, memoryHistory := GetCPUAndMemoryHistory()
			log.Infof("cpuHistory: %v, memoryHistory: %v\n", cpuHistory, memoryHistory)

			e := EstimatorByRatio{}
			newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest := e.Estimate(cpuHistory, memoryHistory)
			log.Infof("newCpuLimit: %v mCore, newCpuRequest: %v mCore, newMemoryLimit: %v bytes, newMemoryRequest:%v bytes\n", newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest)

			err = scaleUpDownPod(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName, newCpuRequest, newMemoryRequest, newCpuLimit, newMemoryLimit)
			if err != nil {
				log.Errorf("scale up/down stateful set error: %v", err)
				continue
			}
			log.Infof("scale up and down successfully")
		}
	}()
}

func (cr *AutoScaleController) Stop() {
	cr.cancelFunc()
}

func (cr *AutoScaleController) doAutoSuspend(clientset *kubernetes.Clientset) {
	if EnableAutoSuspend == false {
		return
	}

	// 1. Collect QPS to determine if scaling in is needed
	qpsHistory := GetQPSHistory()
	log.Infof("qpsHistory: %v\n", qpsHistory)

	if NeedScaleInZero(qpsHistory) {
		ExpectedDataNodeStatefulSetReplicas = 0
	} else {
		ExpectedDataNodeStatefulSetReplicas = 1
	}

	// todo: don't do this every time
	scaleInOutStatefulSet(clientset, AutoScaleClusterNamespace, AutoScaleDataNodeStatefulSetName, ExpectedDataNodeStatefulSetReplicas)
	// Scale in or out the Logger Node
	for _, loggerStatefulSetName := range AutoScaleLoggerNodeStatefulSetName {
		scaleInOutStatefulSet(clientset, AutoScaleClusterNamespace, loggerStatefulSetName, ExpectedDataNodeStatefulSetReplicas)
	}
}
