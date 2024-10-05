package autoscale

import (
	"context"
	"os"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/metrics/pkg/client/clientset/versioned"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

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
	EnableAutoScale                 = false
	EnableAutoSuspend               = false
	AutoScaleDecisionMakingInterval = 5 * time.Second
)

// User Config
var (
	AutoSuspendQpsSampleInterval = 10 * time.Second //todo need to larger than 1s, otherwise vtgate will panic
	AutoSuspendTimeout           = 5 * time.Minute  //todo must larger than AutoSuspendQpsSampleInterval
)

const (
	Milli = 1000
	Gi    = 1024 * 1024 * 1024
	Mi    = 1024 * 1024
	Ki    = 1024
)

// Cluster Config
var (
	//todo All these variables should be read from the environment
	AutoScaleClusterNamespace          = "default"
	AutoScaleDataNodeStatefulSetName   = "mycluster-wesql-0"
	AutoScaleDataNodePodName           = "mycluster-wesql-0-0"
	AutoScaleVTGateHeadlessServiceName = "wesql-vtgate-headless.default.svc.cluster.local"

	AutoScaleLoggerNodeStatefulSetName = []string{"mycluster-wesql-1", "mycluster-wesql-2"}
	AutoScaleLoggerNodePodName         = []string{"mycluster-wesql-1-0", "mycluster-wesql-2-0"}

	AutoSuspendLeaseDuration = 15 * time.Second
	AutoSuspendRenewDeadline = 10 * time.Second
	AutoSuspendRetryPeriod   = 2 * time.Second
	AutoSuspendLeaseName     = "vtgate-autosuspend-leader-election"
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
	fs.StringVar(&AutoScaleVTGateHeadlessServiceName, "auto_scale_vtgate_headless_service_name", AutoScaleVTGateHeadlessServiceName, "auto scale vtgate headless service name")

	fs.DurationVar(&AutoSuspendLeaseDuration, "auto_suspend_lease_duration", AutoSuspendLeaseDuration, "lease duration is the duration that non-leader candidates will wait to force acquire leadership. ")
	fs.DurationVar(&AutoSuspendRenewDeadline, "auto_suspend_renew_deadline", AutoSuspendRenewDeadline, "renew deadline is the duration that the acting master will retry refreshing leadership before giving up.")
	fs.DurationVar(&AutoSuspendRetryPeriod, "auto_suspend_retry_period", AutoSuspendRetryPeriod, "retry period is the duration the LeaderElector clients should wait between tries of actions.")
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

	svc queryservice.QueryService
}

func NewAutoScaleController(svc queryservice.QueryService) *AutoScaleController {
	log.Infof("NewAutoScaleController")
	w := &AutoScaleController{}
	w.ctx, w.cancelFunc = context.WithCancel(context.Background())
	w.svc = svc
	return w
}

func (cr *AutoScaleController) Start() {
	log.Infof("AutoScaleController started")
	clientset, metricsClientset := initK8sClients()

	id, _ := os.Hostname()

	// use Lease as resource lock
	cli := clientset.CoordinationV1()
	if cli == nil {
		log.Errorf("clientset.CoordinationV1() is nil, AutoScaleController start failed")
		return
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      AutoSuspendLeaseName,
			Namespace: AutoScaleClusterNamespace,
		},

		Client: cli,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   AutoSuspendLeaseDuration,
		RenewDeadline:   AutoSuspendRenewDeadline,
		RetryPeriod:     AutoSuspendRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// as leader, do auto suspend and auto scale after resetting context
				cr.ctx, cr.cancelFunc = context.WithCancel(context.Background())
				cr.doAutoSuspendAndAutoScale(clientset, metricsClientset)
			},
			OnStoppedLeading: func() {
				// cancel context when losing leadership
				cr.cancelFunc()
				log.Infof("%s: I am no longer the leader!", id)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					log.Infof("%s: I am the new leader", id)
				} else {
					log.Infof("%s: %s is the new leader", id, identity)
				}
			},
		},
	}

	// start leader election
	go leaderelection.RunOrDie(context.TODO(), leaderElectionConfig)

}

func (cr *AutoScaleController) Stop() {
	cr.cancelFunc()
}

func initK8sClients() (*kubernetes.Clientset, *metricsclientset.Clientset) {
	k8sRestConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Errorf("Error building in-cluster config: %v", err)
		return nil, nil
	}
	clientset, err := kubernetes.NewForConfig(k8sRestConfig)
	if err != nil {
		log.Errorf("Error creating clientset: %v", err)
	}
	// 创建 Metrics 客户端
	metricsClientset, err := metricsclientset.NewForConfig(k8sRestConfig)
	if err != nil {
		log.Errorf("Error creating metrics clientset: %v", err)
	}
	return clientset, metricsClientset
}

func (cr *AutoScaleController) doAutoSuspendAndAutoScale(clientset *kubernetes.Clientset, metricsClientset *versioned.Clientset) {
	for {
		intervalTicker := time.NewTicker(AutoScaleDecisionMakingInterval)
		defer intervalTicker.Stop()
		select {
		case <-cr.ctx.Done():
			log.Infof("AutoScaleController stopped")
			return
		case <-intervalTicker.C:
		case <-WatchActivity():
		}

		if EnableAutoScale == false && EnableAutoSuspend == false {
			// both auto scale and auto suspend are disabled
			continue
		}

		if clientset == nil || metricsClientset == nil {
			log.Errorf("clientset or metricsClientset is nil, reinit them...")
			clientset, metricsClientset = initK8sClients()
		}

		cr.doAutoSuspend(clientset)
		cr.doAutoScale(metricsClientset, clientset)
	}
}

func (cr *AutoScaleController) doAutoSuspend(clientset *kubernetes.Clientset) {
	if clientset == nil {
		log.Errorf("clientset is nil in doAutoSuspend")
		return
	}
	if EnableAutoSuspend == false {
		return
	}

	// 1. Collect last active time from all VTGates to determine if suspend is needed
	lastActiveTimesFromVTGates := GetLastActiveTimestampsFromVTGates()
	log.Infof("lastActiveTimesFromVTGates: %v\n", lastActiveTimesFromVTGates)

	if NeedSuspend(lastActiveTimesFromVTGates) {
		ExpectedDataNodeStatefulSetReplicas = 0
	} else {
		ExpectedDataNodeStatefulSetReplicas = 1
	}

	currentDataNodeReplicas, err := GetStatefulSetReplicaCount(clientset, AutoScaleClusterNamespace, AutoScaleDataNodeStatefulSetName)
	if err != nil {
		log.Errorf("get data node stateful set replica count error: %v", err)
		return
	}
	if currentDataNodeReplicas != ExpectedDataNodeStatefulSetReplicas {
		log.Infof("currentDataNodeReplicas: %v, ExpectedDataNodeStatefulSetReplicas: %v\n", currentDataNodeReplicas, ExpectedDataNodeStatefulSetReplicas)
		scaleInOutStatefulSet(clientset, AutoScaleClusterNamespace, AutoScaleDataNodeStatefulSetName, ExpectedDataNodeStatefulSetReplicas)
	}

	// Scale in or out the Logger Node
	for _, loggerStatefulSetName := range AutoScaleLoggerNodeStatefulSetName {
		currentLoggerNodeReplicas, err := GetStatefulSetReplicaCount(clientset, AutoScaleClusterNamespace, loggerStatefulSetName)
		if err != nil {
			log.Errorf("get logger node stateful set replica count error: %v", err)
			return
		}
		if currentLoggerNodeReplicas != ExpectedDataNodeStatefulSetReplicas {
			log.Infof("currentLoggerNodeName: %v, currentLoggerNodeReplicas: %v, ExpectedDataNodeStatefulSetReplicas: %v\n", loggerStatefulSetName, currentLoggerNodeReplicas, ExpectedDataNodeStatefulSetReplicas)
			scaleInOutStatefulSet(clientset, AutoScaleClusterNamespace, loggerStatefulSetName, ExpectedDataNodeStatefulSetReplicas)
		}
	}
}

func (cr *AutoScaleController) doAutoScale(metricsClientset *versioned.Clientset, clientset *kubernetes.Clientset) {
	if metricsClientset == nil || clientset == nil {
		log.Errorf("metricsClientset or clientset is nil in doAutoScale")
		return
	}
	if ExpectedDataNodeStatefulSetReplicas == 0 || EnableAutoScale == false {
		return
	}

	err := TrackCPUAndMemory(metricsClientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
	if err != nil {
		log.Errorf("track cpu and memory error: %v", err)
		return
	}
	currentCPURequest, currentMemoryRequest, currentCPULimit, currentMemoryLimit, err := GetRequestAndLimitMetrics(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName)
	if err != nil {
		log.Errorf("get request and limit metrics error: %v", err)
		return
	}
	log.Infof("currentCPURequest: %v mCore, currentMemoryRequest: %v bytes, currentCPULimit: %v mCore, currentMemoryLimit :%v bytes\n",
		currentCPURequest, currentMemoryRequest, currentCPULimit, currentMemoryLimit)

	cpuHistory, memoryHistory := GetCPUAndMemoryHistory()
	log.Infof("cpuHistory: %v, memoryHistory: %v\n", cpuHistory, memoryHistory)
	e := EstimatorByRatio{}
	newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest := e.Estimate(cpuHistory, memoryHistory, currentCPURequest, currentMemoryRequest)
	log.Infof("newCpuLimit: %v mCore, newCpuRequest: %v mCore, newMemoryLimit: %v bytes, newMemoryRequest:%v bytes\n", newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest)

	err = scaleUpDownPod(clientset, AutoScaleClusterNamespace, AutoScaleDataNodePodName, newCpuRequest, newMemoryRequest, newCpuLimit, newMemoryLimit)
	if err != nil {
		log.Errorf("scale up/down stateful set error: %v", err)
		return
	}
	log.Infof("scale up and down successfully")
}
