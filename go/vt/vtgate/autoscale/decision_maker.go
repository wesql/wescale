package autoscale

import (
	"github.com/spf13/pflag"
	"math"
	"time"
	"vitess.io/vitess/go/vt/log"
)

// Resource Config
var (
	AutoScaleComputeUnitUpperBound float64 = 10
	AutoScaleComputeUnitLowerBound float64 = 0.25

	AutoScaleCpuComputeUnitRatio      float64 = Core * 1
	AutoScaleMemoryComputeUnitRatio   float64 = Gi * 4
	AutoScaleUseRelaxedCpuMemoryRatio         = false

	// Used by EstimatorByRatio
	AutoScaleCpuRatio    float64 = 0.9
	AutoScaleMemoryRatio float64 = 0.75
)

func RegisterAutoScaleEstimatorFlags(fs *pflag.FlagSet) {
	fs.Float64Var(&AutoScaleComputeUnitUpperBound, "auto_scale_compute_unit_upper_bound", AutoScaleComputeUnitUpperBound, "auto scale will not set compute unit more than auto_scale_compute_unit_upper_bound")
	fs.Float64Var(&AutoScaleComputeUnitLowerBound, "auto_scale_compute_unit_lower_bound", AutoScaleComputeUnitLowerBound, "auto scale will not set compute unit less than auto_scale_compute_unit_lower_bound")

	// Used by EstimatorByRatio
	fs.Float64Var(&AutoScaleCpuRatio, "auto_scale_cpu_ratio", AutoScaleCpuRatio, "The ratio of CPU to adjust during each auto scaling step")
	fs.Float64Var(&AutoScaleMemoryRatio, "auto_scale_memory_ratio", AutoScaleMemoryRatio, "The ratio of memory to adjust during each auto scaling step")
	fs.BoolVar(&AutoScaleUseRelaxedCpuMemoryRatio, "auto_scale_use_relaxed_cpu_memory_ratio", AutoScaleUseRelaxedCpuMemoryRatio, "If true, cpu and memory will be adjusted independently")
}

type EstimatorByRatio struct {
}

// goalCU := max(cpuGoalCU, memGoalCU, lfcGoalCU)
// 1CU = 1CPU + 4G Memory
func (n *EstimatorByRatio) Estimate(cpuHistory CPUHistory, memoryHistory MemoryHistory) (newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest int64) {
	cpuAvg, memoryAvg := calcAvgCpuMemory(cpuHistory, memoryHistory)
	cpuGoalCU := GetSuitableComputeUnit(GetComputeUnitByCpu(float64(cpuAvg) / AutoScaleCpuRatio))
	memGoalCU := GetSuitableComputeUnit(GetComputeUnitByMemory(float64(memoryAvg) / AutoScaleMemoryRatio))

	if AutoScaleUseRelaxedCpuMemoryRatio {
		newCpuLimit = GetCpuByComputeUnit(AutoScaleComputeUnitUpperBound)
		newCpuRequest = GetCpuByComputeUnit(cpuGoalCU)
		newMemoryLimit = GetMemoryByComputeUnit(AutoScaleComputeUnitUpperBound)
		newMemoryRequest = GetMemoryByComputeUnit(memGoalCU)
		return
	}

	goalCU := math.Max(cpuGoalCU, memGoalCU)
	if goalCU > AutoScaleComputeUnitUpperBound {
		goalCU = AutoScaleComputeUnitUpperBound
	} else if goalCU < AutoScaleComputeUnitLowerBound {
		goalCU = AutoScaleComputeUnitLowerBound
	}

	newCpuLimit = GetCpuByComputeUnit(AutoScaleComputeUnitUpperBound)
	newCpuRequest = GetCpuByComputeUnit(goalCU)
	newMemoryLimit = GetMemoryByComputeUnit(AutoScaleComputeUnitUpperBound)
	newMemoryRequest = GetMemoryByComputeUnit(goalCU)
	return
}

func GetSecondsByDuration(p *time.Duration) int {
	return int(*p / time.Second)
}

func GetQpsSampleHistoryLength() int {
	// todo: do not use division here, fixme
	return GetSecondsByDuration(&AutoSuspendTimeout) / GetSecondsByDuration(&AutoSuspendQpsSampleInterval)
}

func NeedScaleInZero(history QPSHistory) bool {
	if len(history) < GetQpsSampleHistoryLength() {
		return false
	}
	for _, v := range history {
		if v > 0 {
			return false
		}
	}
	return true
}

// GetSuitableComputeUnit returns the ceil of the suitable compute unit
func GetSuitableComputeUnit(cu float64) float64 {
	computeUnitList := []float64{
		0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
		51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
		61, 62, 63, 64,
	}
	// find the smallest compute unit that is larger than cu
	for _, v := range computeUnitList {
		if v >= cu {
			return v
		}
	}
	log.Warningf("compute unit %v is too large, using the largest compute unit %v", cu, computeUnitList[len(computeUnitList)-1])
	return computeUnitList[len(computeUnitList)-1]
}

func GetComputeUnitByCpu(cpu float64) float64 {
	return cpu / AutoScaleCpuComputeUnitRatio
}

func GetComputeUnitByMemory(memory float64) float64 {
	return memory / AutoScaleMemoryComputeUnitRatio
}

func GetCpuByComputeUnit(cu float64) int64 {
	return int64(cu * AutoScaleCpuComputeUnitRatio)
}

func GetMemoryByComputeUnit(cu float64) int64 {
	return int64(cu * AutoScaleMemoryComputeUnitRatio)
}
