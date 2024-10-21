package autoscale

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"vitess.io/vitess/go/vt/log"
)

func scaleInOutStatefulSet(clientset *kubernetes.Clientset, namespace string, statefulSetName string, replicas int32) error {
	statefulSetsClient := clientset.AppsV1().StatefulSets(namespace)

	statefulSet, err := statefulSetsClient.Get(context.TODO(), statefulSetName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Error getting StatefulSet %s: %s", statefulSetName, err)
		return err
	}

	statefulSet.Spec.Replicas = &replicas

	_, err = statefulSetsClient.Update(context.TODO(), statefulSet, metav1.UpdateOptions{})
	if err != nil {
		log.Errorf("Error updating StatefulSet %s: %s", statefulSetName, err)
	} else {
		fmt.Printf("Successfully updated StatefulSet %s.\n", statefulSetName)
	}
	return err
}

func scaleUpDownPod(clientset *kubernetes.Clientset, namespace string, podName string,
	cpuRequest, memoryRequest, cpuLimit, memoryLimit int64) error {
	podsClient := clientset.CoreV1().Pods(namespace)

	pod, err := podsClient.Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	for i, container := range pod.Spec.Containers {

		if container.Name == AutoScaleMysqlContainerName {
			pod.Spec.Containers[i].Resources.Requests = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cpuRequest, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memoryRequest, resource.BinarySI),
			}
			pod.Spec.Containers[i].Resources.Limits = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cpuLimit, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(memoryLimit, resource.BinarySI),
			}
		}
	}

	_, err = podsClient.Update(context.TODO(), pod, metav1.UpdateOptions{})
	if err == nil {
		fmt.Printf("Successfully updated resources for Pod %s.\n", podName)
	}
	return err
}
