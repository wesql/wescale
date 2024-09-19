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
	EnableAutoScale                 = true
	AutoScaleDecisionMakingInterval = 5 * time.Second
)

// User Config
var (
	AutoSuspendTimeout = 5 * time.Minute
)

// Resource Config
var (
	AutoScaleCpuUpperBound    int64 = 4000
	AutoScaleCpuLowerBound    int64 = 500
	AutoScaleMemoryUpperBound int64 = 5000
	AutoScaleMemoryLowerBound int64 = 500
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
	fs.BoolVar(&EnableAutoScale, "enable_auto_scale", EnableAutoScale, "enable auto scaling")
	fs.DurationVar(&AutoScaleDecisionMakingInterval, "auto_scale_decision_making_interval", AutoScaleDecisionMakingInterval, "auto scale decision making interval")
	// Resource Config
	fs.Int64Var(&AutoScaleCpuUpperBound, "auto_scale_cpu_upper_bound", AutoScaleCpuUpperBound, "auto scale will not set cpu more than auto_scale_cpu_upper_bound")
	fs.Int64Var(&AutoScaleCpuLowerBound, "auto_scale_cpu_lower_bound", AutoScaleCpuLowerBound, "auto scale will not set cpu less than auto_scale_cpu_lower_bound")
	fs.Int64Var(&AutoScaleMemoryUpperBound, "auto_scale_memory_upper_bound", AutoScaleMemoryUpperBound, "auto scale will not set memory more than auto_scale_memory_upper_bound")
	fs.Int64Var(&AutoScaleMemoryLowerBound, "auto_scale_memory_lower_bound", AutoScaleMemoryLowerBound, "auto scale will not set memory less than auto_scale_memory_lower_bound")
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
			clientset, err := kubernetes.NewForConfig(cr.config)
			if err != nil {
				log.Errorf("Error creating clientset: %s", err.Error())
				continue
			}

			// 1. 搜集qps，判断是否需要scale in
			qpsHistory := GetQPSHistory()
			log.Infof("qpsHistory: %v\n", qpsHistory)

			if NeedScaleInZero(qpsHistory) {
				DataNodeStatefulSetReplicas = 0
			} else {
				DataNodeStatefulSetReplicas = 1
			}

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
				continue
			}
			if CurrentDataNodeStatefulSetReplicas == 0 {
				// no need to scale up or down
				continue
			}

			// 2. 搜集cpu和memory，判断是否需要scale up/down
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
			log.Infof("totalCPURequest: %v, totalMemoryRequest: %v, totalCPULimit: %v, totalMemoryLimit :%v\n",
				totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit)

			cpuHistory, memoryHistory := GetCPUAndMemoryHistory()
			log.Infof("cpuHistory: %v, memoryHistory: %v\n", cpuHistory, memoryHistory)

			e := NaiveEstimator{
				CPUUpperMargin:    500,
				CPULowerMargin:    500,
				MemoryUpperMargin: 500,
				MemoryLowerMargin: 500,
				CPUDelta:          500,
				MemoryDelta:       500,
			}
			cpuUpper, cpuLower, memoryUpper, memoryLower := e.Estimate(cpuHistory, totalCPULimit, totalCPURequest, AutoScaleCpuUpperBound, AutoScaleCpuLowerBound,
				memoryHistory, totalMemoryLimit, totalMemoryRequest, AutoScaleMemoryUpperBound, AutoScaleMemoryLowerBound)
			log.Infof("cpuUpper: %v, cpuLower: %v, memoryUpper: %v, memoryLower:%v \n", cpuUpper, cpuLower, memoryUpper, memoryLower)

			// todo, here we just scale up/down cpu
			err = scaleUpDownPod(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName, cpuLower, totalMemoryRequest, cpuUpper, totalMemoryLimit)
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
