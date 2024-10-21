package autoscale

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	versioned "k8s.io/metrics/pkg/client/clientset/versioned"

	"vitess.io/vitess/go/stats"
)

type CPUHistory []int64
type MemoryHistory []int64

const (
	HistorySize = 10
)

var (
	cpuHistoryRing    stats.RingInt64
	memoryHistoryRing stats.RingInt64
)

func init() {
	cpuHistoryRing = *stats.NewRingInt64(HistorySize)
	memoryHistoryRing = *stats.NewRingInt64(HistorySize)
}

func TrackCPUAndMemory(metricsClientset *versioned.Clientset, namespae, targetPod string) error {
	totalCPUUsage, totalMemoryUsage, err := GetRealtimeMetrics(metricsClientset, namespae, targetPod)
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

func GetRealtimeMetrics(metricsClientset *versioned.Clientset, namespace, targetPod string) (int64, int64, error) {
	podMetrics, err := metricsClientset.MetricsV1beta1().PodMetricses(namespace).Get(context.TODO(), targetPod, metav1.GetOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("fail to get pod metrics info: %v", err)
	}

	var totalCPUUsage int64 = 0
	var totalMemoryUsage int64 = 0

	for _, container := range podMetrics.Containers {
		cpuQuantity := container.Usage.Cpu()
		memQuantity := container.Usage.Memory()

		totalCPUUsage += cpuQuantity.MilliValue() // the CPU unit：milli-core
		totalMemoryUsage += memQuantity.Value()   // the memory unit: byte
	}
	return totalCPUUsage, totalMemoryUsage, nil
}

func GetRequestAndLimitMetrics(clientset *kubernetes.Clientset, namespace, targetPod string) (int64, int64, int64, int64, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), targetPod, metav1.GetOptions{})
	if err != nil {
		return 0, 0, 0, 0, err
	}

	var totalCPURequest, totalCPULimit, totalMemoryRequest, totalMemoryLimit int64

	for _, container := range pod.Spec.Containers {
		cpuRequest := container.Resources.Requests[v1.ResourceCPU]
		cpuLimit := container.Resources.Limits[v1.ResourceCPU]

		memRequest := container.Resources.Requests[v1.ResourceMemory]
		memLimit := container.Resources.Limits[v1.ResourceMemory]

		// the CPU unit is milli-core and the memory unit is byte
		totalCPURequest += cpuRequest.MilliValue()
		totalCPULimit += cpuLimit.MilliValue()
		totalMemoryRequest += memRequest.Value()
		totalMemoryLimit += memLimit.Value()
	}

	return totalCPURequest, totalMemoryRequest, totalCPULimit, totalMemoryLimit, nil
}

func GetStatefulSetReplicaCount(clientset *kubernetes.Clientset, namespace string, statefulSetName string) (int32, error) {
	statefulSetClient := clientset.AppsV1().StatefulSets(namespace)
	statefulSet, err := statefulSetClient.Get(context.TODO(), statefulSetName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get StatefulSet: %v", err)
	}
	return *statefulSet.Spec.Replicas, nil
}

func calcAvgCpuMemory(cpuHistory CPUHistory, memoryHistory MemoryHistory) (int64, int64) {
	cpuTotal := int64(0)
	memoryTotal := int64(0)
	for i := range cpuHistory {
		cpuTotal += cpuHistory[i]
		memoryTotal += memoryHistory[i]
	}
	cpuAvg := cpuTotal / int64(len(cpuHistory))
	memoryAvg := memoryTotal / int64(len(memoryHistory))
	return cpuAvg, memoryAvg
}
