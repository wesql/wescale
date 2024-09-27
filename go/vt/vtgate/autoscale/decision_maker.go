package autoscale

import "github.com/spf13/pflag"

type Estimator interface {
	Estimate(cpuHistory CPUHistory, cpuUpperLimit, cpuLowerLimit, cpuMax, cpuMin int64,
		memoryHistory MemoryHistory, memoryUpperLimit, memoryLowerLimit, memoryMax, memoryMin int64) (int64, int64, int64, int64)
}

func RegisterAutoScaleEstimatorFlags(fs *pflag.FlagSet) {
	// EstimatorByDelta Config
	fs.Int64Var(&AutoScaleCpuUpperBoundInMillicores, "auto_scale_cpu_upper_bound_in_milli_cores", AutoScaleCpuUpperBoundInMillicores, "auto scale will not set cpu more than auto_scale_cpu_upper_bound")
	fs.Int64Var(&AutoScaleCpuLowerBoundInMillicores, "auto_scale_cpu_lower_bound_in_milli_cores", AutoScaleCpuLowerBoundInMillicores, "auto scale will not set cpu less than auto_scale_cpu_lower_bound")
	fs.Int64Var(&AutoScaleMemoryUpperBoundInBytes, "auto_scale_memory_upper_bound_in_bytes", AutoScaleMemoryUpperBoundInBytes, "auto scale will not set memory more than auto_scale_memory_upper_bound")
	fs.Int64Var(&AutoScaleMemoryLowerBoundInBytes, "auto_scale_memory_lower_bound_in_bytes", AutoScaleMemoryLowerBoundInBytes, "auto scale will not set memory less than auto_scale_memory_lower_bound")

	fs.Int64Var(&AutoScaleCpuUpperMarginInMillicores, "auto_scale_cpu_upper_margin_in_milli_cores", AutoScaleCpuUpperMarginInMillicores, "Auto scale will not increase CPU more than this upper margin")
	fs.Int64Var(&AutoScaleCpuLowerMarginInMillicores, "auto_scale_cpu_lower_margin_in_milli_cores", AutoScaleCpuLowerMarginInMillicores, "Auto scale will not decrease CPU less than this lower margin")
	fs.Int64Var(&AutoScaleMemoryUpperMarginInBytes, "auto_scale_memory_upper_margin_in_bytes", AutoScaleMemoryUpperMarginInBytes, "Auto scale will not increase memory more than this upper margin")
	fs.Int64Var(&AutoScaleMemoryLowerMarginInBytes, "auto_scale_memory_lower_margin_in_bytes", AutoScaleMemoryLowerMarginInBytes, "Auto scale will not decrease memory less than this lower margin")
	fs.Int64Var(&AutoScaleCpuDeltaInMillicores, "auto_scale_cpu_delta_in_milli_cores", AutoScaleCpuDeltaInMillicores, "The CPU amount to adjust during each auto scaling step")
	fs.Int64Var(&AutoScaleMemoryDeltaInBytes, "auto_scale_memory_delta_in_bytes", AutoScaleMemoryDeltaInBytes, "The memory amount to adjust during each auto scaling step")
}

// Resource Config
var (
	AutoScaleCpuUpperBoundInMillicores int64 = 4000     // mCore
	AutoScaleCpuLowerBoundInMillicores int64 = 500      // mCore
	AutoScaleMemoryUpperBoundInBytes   int64 = 5 * Gi   // bytes
	AutoScaleMemoryLowerBoundInBytes   int64 = 0.5 * Gi // bytes

	AutoScaleCpuUpperMarginInMillicores int64 = 500      // mCore
	AutoScaleCpuLowerMarginInMillicores int64 = 500      // mCore
	AutoScaleMemoryUpperMarginInBytes   int64 = 500 * Mi // bytes
	AutoScaleMemoryLowerMarginInBytes   int64 = 500 * Mi // bytes
	AutoScaleCpuDeltaInMillicores       int64 = 500      // mCore
	AutoScaleMemoryDeltaInBytes         int64 = 500 * Mi // bytes
)

type EstimatorByDelta struct {
	CPUUpperMargin int64
	CPULowerMargin int64

	MemoryUpperMargin int64
	MemoryLowerMargin int64

	CPUDelta    int64
	MemoryDelta int64
}

func (n *EstimatorByDelta) Estimate(cpuHistory CPUHistory, cpuUpperLimit, cpuLowerLimit, cpuMax, cpuMin int64,
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

type EstimatorByRatio struct {
	AutoScaleCPURatio    float64
	AutoScaleMemoryRatio float64
}

func (n *EstimatorByRatio) Estimate(cpuHistory CPUHistory, cpuUpperLimit, cpuLowerLimit, cpuMax, cpuMin int64,
	memoryHistory MemoryHistory, memoryUpperLimit, memoryLowerLimit, memoryMax, memoryMin int64) (int64, int64, int64, int64) {
	return 0, 0, 0, 0
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
