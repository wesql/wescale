package autoscale

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
	"vitess.io/vitess/go/stats"
)

type QPSHistory []float64
type CPUHistory []int64
type MemoryHistory []int64

const (
	HistorySize = 10
)

var (
	cpuHistoryRing    stats.RingInt64
	memoryHistoryRing stats.RingInt64
	QPSByDbType       *stats.Rates
)

var qpsHistoryChanged = make(chan struct{}, 1)

func NotifyQPSHistoryChange() {
	select {
	case qpsHistoryChanged <- struct{}{}:
	default:
	}
}

func WatchQPSHistoryChange() <-chan struct{} {
	return qpsHistoryChanged
}

func init() {
	cpuHistoryRing = *stats.NewRingInt64(HistorySize)
	memoryHistoryRing = *stats.NewRingInt64(HistorySize)
}

func TrackCPUAndMemory(config *rest.Config, namespae, targetPod string) error {
	totalCPUUsage, totalMemoryUsage, err := GetRealtimeMetrics(config, namespae, targetPod)
	if err != nil {
		return err
	}
	cpuHistoryRing.Add(totalCPUUsage)
	memoryHistoryRing.Add(totalMemoryUsage)
	return nil
}

func GetCPUAndMemoryHistory() (CPUHistory, MemoryHistory) {
	return cpuHistoryRing.Values(), memoryHistoryRing.Values()
}

func GetRealtimeMetrics(config *rest.Config, namespace, targetPod string) (int64, int64, error) {
	// 创建 Metrics 客户端
	metricsClientset, err := metricsclientset.NewForConfig(config)
	if err != nil {
		return 0, 0, fmt.Errorf("fail to create metrics client: %v", err)
	}

	// 获取 Pod 的指标信息
	podMetrics, err := metricsClientset.MetricsV1beta1().PodMetricses(namespace).Get(context.TODO(), targetPod, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("fail to get pod metrics info: %v", err)
	}

	// 累加所有容器的 CPU 和内存使用量
	var totalCPUUsage int64 = 0
	var totalMemoryUsage int64 = 0

	for _, container := range podMetrics.Containers {
		cpuQuantity := container.Usage.Cpu()
		memQuantity := container.Usage.Memory()

		totalCPUUsage += cpuQuantity.MilliValue() // CPU 使用量（单位：毫核）
		totalMemoryUsage += memQuantity.Value()   // 内存使用量（单位：字节）
	}
	return totalCPUUsage, totalMemoryUsage, nil
}

func GetRequestAndLimitMetrics(config *rest.Config, namespace, targetPod string) (int64, int64, int64, int64, error) {
	// 创建 Kubernetes 客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	// 获取指定 Pod
	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), targetPod, metav1.GetOptions{})
	if err != nil {
		return 0, 0, 0, 0, err
	}

	var totalCPURequest, totalCPULimit, totalMemoryRequest, totalMemoryLimit int64

	// 遍历 Pod 中的每个容器，获取 CPU 和内存的 Request 和 Limit 信息
	for _, container := range pod.Spec.Containers {
		// 获取 CPU 请求和限制
		cpuRequest := container.Resources.Requests[v1.ResourceCPU]
		cpuLimit := container.Resources.Limits[v1.ResourceCPU]

		// 获取内存请求和限制
		memRequest := container.Resources.Requests[v1.ResourceMemory]
		memLimit := container.Resources.Limits[v1.ResourceMemory]

		// 转换为毫核和字节单位
		totalCPURequest += cpuRequest.MilliValue()
		totalCPULimit += cpuLimit.MilliValue()
		totalMemoryRequest += memRequest.Value()
		totalMemoryLimit += memLimit.Value()
	}

	return totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit, nil
}

func GetQPSHistory() QPSHistory {
	return QPSByDbType.Get()["All"]
}

func GetStatefulSetReplicaCount(clientset *kubernetes.Clientset, namespace string, statefulSetName string) (int32, error) {
	statefulSetClient := clientset.AppsV1().StatefulSets(namespace)
	statefulSet, err := statefulSetClient.Get(context.TODO(), statefulSetName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get StatefulSet: %v", err)
	}
	return *statefulSet.Spec.Replicas, nil
}
