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

// System Config
var (
	EnableAutoSuspend               = true
	AutoScaleDecisionMakingInterval = 5 * time.Second
	EnableAutoScale                 = true
)

// User Config
var (
	AutoSuspendTimeout = 5 * time.Minute
)

const (
	Gi = 1024 * 1024 * 1024
	Mi = 1024 * 1024
	Ki = 1024
)

// Resource Config
var (
	AutoScaleCpuUpperBound    int64 = 4000     // mCore
	AutoScaleCpuLowerBound    int64 = 500      // mCore
	AutoScaleMemoryUpperBound int64 = 5 * Gi   // bytes
	AutoScaleMemoryLowerBound int64 = 0.5 * Gi // bytes

	AutoScaleCpuUpperMargin    int64 = 500      // mCore
	AutoScaleCpuLowerMargin    int64 = 500      // mCore
	AutoScaleMemoryUpperMargin int64 = 500 * Mi // bytes
	AutoScaleMemoryLowerMargin int64 = 500 * Mi // bytes
	AutoScaleCpuDelta          int64 = 500      // mCore
	AutoScaleMemoryDelta       int64 = 500 * Mi // bytes
)

// Cluster Config
var (
	AutoScaleClusterNamespace        = "default"
	AutoScaleDataNodeStatefulSetName = "mycluster-mysql-0"
	AutoScaleDataNodePodName         = "mycluster-mysql-0-0"

	AutoScaleLoggerNodeStatefulSetName = []string{"mycluster-mysql-1", "mycluster-mysql-2"}
	AutoScaleLoggerNodePodName         = []string{"mycluster-mysql-1-0", "mycluster-mysql-2-0"}
)

// The number of replicas of the DataNode's StatefulSet that we want to have
var DataNodeStatefulSetReplicas int32 = 1

// The number of replicas of the DataNode's StatefulSet that currently exist
var CurrentDataNodeStatefulSetReplicas int32 = 1

func RegisterAutoScaleFlags(fs *pflag.FlagSet) {
	// System Config
	fs.BoolVar(&EnableAutoSuspend, "enable_auto_scale", EnableAutoSuspend, "enable auto suspend")
	fs.DurationVar(&AutoScaleDecisionMakingInterval, "auto_scale_decision_making_interval", AutoScaleDecisionMakingInterval, "auto scale decision making interval")
	fs.BoolVar(&EnableAutoScale, "enable_auto_scale_up_down", EnableAutoScale, "enable auto scaling up and down")
	// Resource Config
	fs.Int64Var(&AutoScaleCpuUpperBound, "auto_scale_cpu_upper_bound", AutoScaleCpuUpperBound, "auto scale will not set cpu more than auto_scale_cpu_upper_bound")
	fs.Int64Var(&AutoScaleCpuLowerBound, "auto_scale_cpu_lower_bound", AutoScaleCpuLowerBound, "auto scale will not set cpu less than auto_scale_cpu_lower_bound")
	fs.Int64Var(&AutoScaleMemoryUpperBound, "auto_scale_memory_upper_bound", AutoScaleMemoryUpperBound, "auto scale will not set memory more than auto_scale_memory_upper_bound")
	fs.Int64Var(&AutoScaleMemoryLowerBound, "auto_scale_memory_lower_bound", AutoScaleMemoryLowerBound, "auto scale will not set memory less than auto_scale_memory_lower_bound")

	fs.Int64Var(&AutoScaleCpuUpperMargin, "auto_scale_cpu_upper_margin", AutoScaleCpuUpperMargin, "Auto scale will not increase CPU more than this upper margin")
	fs.Int64Var(&AutoScaleCpuLowerMargin, "auto_scale_cpu_lower_margin", AutoScaleCpuLowerMargin, "Auto scale will not decrease CPU less than this lower margin")
	fs.Int64Var(&AutoScaleMemoryUpperMargin, "auto_scale_memory_upper_margin", AutoScaleMemoryUpperMargin, "Auto scale will not increase memory more than this upper margin")
	fs.Int64Var(&AutoScaleMemoryLowerMargin, "auto_scale_memory_lower_margin", AutoScaleMemoryLowerMargin, "Auto scale will not decrease memory less than this lower margin")
	fs.Int64Var(&AutoScaleCpuDelta, "auto_scale_cpu_delta", AutoScaleCpuDelta, "The CPU amount to adjust during each auto scaling step")
	fs.Int64Var(&AutoScaleMemoryDelta, "auto_scale_memory_delta", AutoScaleMemoryDelta, "The memory amount to adjust during each auto scaling step")

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
			if cr.doAutoSuspend(clientset) {
				continue
			}

			// 2. Collect CPU and memory metrics to determine if scaling up/down is needed
			if CurrentDataNodeStatefulSetReplicas == 0 || !EnableAutoScale {
				// No need to scale up or down
				continue
			}

			err = TrackCPUAndMemory(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("track cpu and memory error: %v", err)
				continue
			}
			totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit, err := GetRequestAndLimitMetrics(cr.config, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
			if err != nil {
				log.Errorf("get request and limit metrics error: %v", err)
				continue
			}
			log.Infof("totalCPURequest: %v mCore, totalMemoryRequest: %v bytes, totalCPULimit: %v mCore, totalMemoryLimit :%v bytes\n",
				totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit)

			cpuHistory, memoryHistory := GetCPUAndMemoryHistory()
			log.Infof("cpuHistory: %v, memoryHistory: %v\n", cpuHistory, memoryHistory)

			e := NaiveEstimator{
				CPUUpperMargin:    AutoScaleCpuUpperMargin,
				CPULowerMargin:    AutoScaleCpuLowerMargin,
				MemoryUpperMargin: AutoScaleMemoryUpperMargin,
				MemoryLowerMargin: AutoScaleMemoryLowerMargin,
				CPUDelta:          AutoScaleCpuDelta,
				MemoryDelta:       AutoScaleMemoryDelta,
			}
			cpuUpper, cpuLower, memoryUpper, memoryLower := e.Estimate(cpuHistory, totalCPULimit, totalCPURequest, AutoScaleCpuUpperBound, AutoScaleCpuLowerBound,
				memoryHistory, totalMemoryLimit, totalMemoryRequest, AutoScaleMemoryUpperBound, AutoScaleMemoryLowerBound)
			log.Infof("cpuUpper: %v mCore, cpuLower: %v mCore, memoryUpper: %v bytes, memoryLower:%v bytes\n", cpuUpper, cpuLower, memoryUpper, memoryLower)

			err = scaleUpDownPod(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName, cpuLower, memoryLower, cpuUpper, memoryUpper)
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

func (cr *AutoScaleController) doAutoSuspend(clientset *kubernetes.Clientset) bool {
	if !EnableAutoSuspend {
		return false
	}

	// 1. Collect QPS to determine if scaling in is needed
	qpsHistory := GetQPSHistory()
	log.Infof("qpsHistory: %v\n", qpsHistory)

	if NeedScaleInZero(qpsHistory) {
		DataNodeStatefulSetReplicas = 0
	} else {
		DataNodeStatefulSetReplicas = 1
	}

	var err error
	CurrentDataNodeStatefulSetReplicas, err = GetStatefulSetReplicaCount(clientset, AutoScaleClusterNamespace, AutoScaleDataNodeStatefulSetName)
	if err != nil {
		log.Errorf("Error getting stateful set replicas: %s", err.Error())
	}
	log.Infof("CurrentDataNodeStatefulSetReplicas: %v\n", CurrentDataNodeStatefulSetReplicas)

	if CurrentDataNodeStatefulSetReplicas != DataNodeStatefulSetReplicas {
		err = scaleInOutStatefulSet(clientset, AutoScaleClusterNamespace, AutoScaleDataNodeStatefulSetName, DataNodeStatefulSetReplicas)
		if err != nil {
			log.Errorf("Error scale in to zero: %s", err.Error())
		} else {
			log.Infof("scale in/out from %v to %v successfully", CurrentDataNodeStatefulSetReplicas, DataNodeStatefulSetReplicas)
		}
		return true // Indicate that we should continue the loop
	}

	return false
}
