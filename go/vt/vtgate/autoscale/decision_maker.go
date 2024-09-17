package autoscale

type Estimator interface {
	Estimate(cpuHistory CPUHistory, cpuUpperLimit, cpuLowerLimit, cpuMax, cpuMin int64,
		memoryHistory MemoryHistory, memoryUpperLimit, memoryLowerLimit, memoryMax, memoryMin int64) (int64, int64, int64, int64)
}

type NaiveEstimator struct {
	CPUUpperMargin int64
	CPULowerMargin int64

	MemoryUpperMargin int64
	MemoryLowerMargin int64

	CPUDelta    int64
	MemoryDelta int64
}

func (n *NaiveEstimator) Estimate(cpuHistory CPUHistory, cpuUpperLimit, cpuLowerLimit, cpuMax, cpuMin int64,
	memoryHistory MemoryHistory, memoryUpperLimit, memoryLowerLimit, memoryMax, memoryMin int64) (int64, int64, int64, int64) {
	cpuTotal := int64(0)
	memoryTotal := int64(0)
	for i, _ := range cpuHistory {
		cpuTotal += cpuHistory[i]
		memoryTotal += memoryHistory[i]
	}
	cpuAVG := cpuTotal / int64(len(cpuHistory))
	memoryAVG := memoryTotal / int64(len(memoryHistory))

	suggestCPUUpper := cpuUpperLimit
	suggestCPULower := cpuLowerLimit

	suggestMemoryUpper := memoryUpperLimit
	suggestMemoryLower := memoryLowerLimit

	if cpuAVG > cpuUpperLimit-n.CPUUpperMargin {
		suggestCPUUpper = cpuAVG + n.CPUDelta
		if suggestCPUUpper > cpuMax {
			suggestCPUUpper = cpuUpperLimit
			suggestCPULower = cpuMax
		}
	}
	if cpuAVG < cpuLowerLimit-n.CPULowerMargin {
		suggestCPULower = cpuAVG + n.CPUDelta
		if suggestCPULower < cpuMin {
			suggestCPULower = cpuMin
		}
	}

	if memoryAVG > memoryUpperLimit-n.MemoryUpperMargin {
		suggestMemoryUpper = memoryAVG + n.MemoryDelta
		if suggestMemoryUpper > memoryMax {
			suggestMemoryUpper = memoryMax
		}
	}
	if memoryAVG < memoryLowerLimit-n.MemoryLowerMargin {
		suggestMemoryLower = memoryAVG + n.MemoryDelta
		if suggestMemoryLower < memoryMin {
			suggestMemoryLower = memoryMin
		}
	}
	return suggestCPUUpper, suggestCPULower, suggestMemoryUpper, suggestMemoryLower
}

var (
	QpsSampleIntervalSeconds = 10
	QpsSampleHistoryLength   = 5 * 60 / QpsSampleIntervalSeconds
)

func NeedScaleInZero(history QPSHistory) bool {
	if len(history) < QpsSampleHistoryLength {
		return false
	}
	for _, v := range history {
		if v > 0 {
			return false
		}
	}
	return true
}
