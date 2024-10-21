package autoscale

import (
	"math"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

// Resource Config
var (
	AutoScaleComputeUnitUpperBound float64 = 10
	AutoScaleComputeUnitLowerBound float64 = 0.25

	AutoScaleCpuMilliCoreComputeUnitRatio float64 = Milli * 1
	AutoScaleMemoryByteComputeUnitRatio   float64 = Gi * 4
	AutoScaleUseRelaxedCpuMemoryRatio             = false

	// Used by EstimatorByRatio
	AutoScaleCpuRatio float64 = 0.9
	// cpu request will remains the same if (the average cpu load) / (current cpu request)
	// is between upper bound and lower bound, upper bound should be greater than lower bound,
	// what's more, AutoScaleCpuNoAdjustUpperBoundRatio should be greather than AutoScaleCpuRatio,
	// and AutoScaleCpuNoAdjustLowerBoundRatio should be less than AutoScaleCpuRatio.
	AutoScaleCpuNoAdjustUpperBoundRatio float64 = 0.9
	AutoScaleCpuNoAdjustLowerBoundRatio float64 = 0.8

	AutoScaleMemoryRatio                   float64 = 0.75
	AutoScaleMemoryNoAdjustUpperBoundRatio float64 = 0.75
	AutoScaleMemoryNoAdjustLowerBoundRatio float64 = 0.6
)

func RegisterAutoScaleEstimatorFlags(fs *pflag.FlagSet) {
	fs.Float64Var(&AutoScaleComputeUnitUpperBound, "auto_scale_compute_unit_upper_bound", AutoScaleComputeUnitUpperBound, "auto scale will not set compute unit more than auto_scale_compute_unit_upper_bound")
	fs.Float64Var(&AutoScaleComputeUnitLowerBound, "auto_scale_compute_unit_lower_bound", AutoScaleComputeUnitLowerBound, "auto scale will not set compute unit less than auto_scale_compute_unit_lower_bound")

	// Used by EstimatorByRatio
	fs.Float64Var(&AutoScaleCpuRatio, "auto_scale_cpu_ratio", AutoScaleCpuRatio, "The ratio of CPU to adjust during each auto scaling step")
	fs.Float64Var(&AutoScaleMemoryRatio, "auto_scale_memory_ratio", AutoScaleMemoryRatio, "The ratio of memory to adjust during each auto scaling step")
	fs.BoolVar(&AutoScaleUseRelaxedCpuMemoryRatio, "auto_scale_use_relaxed_cpu_memory_ratio", AutoScaleUseRelaxedCpuMemoryRatio, "If true, cpu and memory will be adjusted independently")
	fs.Float64Var(&AutoScaleCpuNoAdjustUpperBoundRatio, "auto_scale_cpu_no_adjust_upper_bound_ratio", AutoScaleCpuNoAdjustUpperBoundRatio, "cpu request will remains the same if (the average cpu load) / (current cpu request) is between upper bound and lower bound")
	fs.Float64Var(&AutoScaleCpuNoAdjustLowerBoundRatio, "auto_scale_cpu_no_adjust_lower_bound_ratio", AutoScaleCpuNoAdjustLowerBoundRatio, "cpu request will remains the same if (the average cpu load) / (current cpu request) is between upper bound and lower bound")
	fs.Float64Var(&AutoScaleMemoryNoAdjustUpperBoundRatio, "auto_scale_memory_no_adjust_upper_bound_ratio", AutoScaleMemoryNoAdjustUpperBoundRatio, "memory request will remains the same if (the average memory load) / (current memory request) is between upper bound and lower bound")
	fs.Float64Var(&AutoScaleMemoryNoAdjustLowerBoundRatio, "auto_scale_memory_no_adjust_lower_bound_ratio", AutoScaleMemoryNoAdjustLowerBoundRatio, "memory request will remains the same if (the average memory load) / (current memory request) is between upper bound and lower bound")
}

type EstimatorByRatio struct {
}

// goalCU := max(cpuGoalCU, memGoalCU, lfcGoalCU)
// 1CU = 1CPU + 4G Memory
func (n *EstimatorByRatio) Estimate(cpuHistory CPUHistory, memoryHistory MemoryHistory, currentCPURequest, currentMemoryRequest int64) (newCpuLimit, newCpuRequest, newMemoryLimit, newMemoryRequest int64) {
	cpuAvg, memoryAvg := calcAvgCpuMemory(cpuHistory, memoryHistory)

	var cpuGoalCU float64
	var memGoalCU float64
	// don't need adjust
	if int64(float64(currentCPURequest)*AutoScaleCpuNoAdjustLowerBoundRatio) < cpuAvg &&
		cpuAvg < int64(float64(currentCPURequest)*AutoScaleCpuNoAdjustUpperBoundRatio) {
		cpuGoalCU = GetSuitableComputeUnit(GetComputeUnitByCpu(float64(currentCPURequest) / AutoScaleCpuRatio))
	} else {
		cpuGoalCU = GetSuitableComputeUnit(GetComputeUnitByCpu(float64(cpuAvg) / AutoScaleCpuRatio))
	}

	if int64(float64(currentMemoryRequest)*AutoScaleMemoryNoAdjustLowerBoundRatio) < memoryAvg &&
		memoryAvg < int64(float64(currentMemoryRequest)*AutoScaleMemoryNoAdjustUpperBoundRatio) {
		memGoalCU = GetSuitableComputeUnit(GetComputeUnitByCpu(float64(currentMemoryRequest) / AutoScaleMemoryRatio))
	} else {
		memGoalCU = GetSuitableComputeUnit(GetComputeUnitByMemory(float64(memoryAvg) / AutoScaleMemoryRatio))
	}

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

func NeedSuspend(lastActiveTimestampsFromAllVTGates []int64) bool {
	lastActiveTimeOfCurrentVTGate := time.Unix(LastActiveTimestamp.Get(), 0)
	if time.Since(lastActiveTimeOfCurrentVTGate) < AutoSuspendTimeout {
		return false
	}

	if len(lastActiveTimestampsFromAllVTGates) == 0 {
		log.Errorf("lastActiveTimestampsFromAllVTGates is empty")
		return false
	}
	// If at least one of vtgate received queries in the last AutoSuspendTimeout duration,
	// don't need to suspend
	needSuspend := true
	for _, lastTimestamp := range lastActiveTimestampsFromAllVTGates {
		lastTime := time.Unix(lastTimestamp, 0)
		if time.Since(lastTime) < AutoSuspendTimeout {
			needSuspend = false
		}
	}

	return needSuspend
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
	return cpu / AutoScaleCpuMilliCoreComputeUnitRatio
}

func GetComputeUnitByMemory(memory float64) float64 {
	return memory / AutoScaleMemoryByteComputeUnitRatio
}

func GetCpuByComputeUnit(cu float64) int64 {
	return int64(cu * AutoScaleCpuMilliCoreComputeUnitRatio)
}

func GetMemoryByComputeUnit(cu float64) int64 {
	return int64(cu * AutoScaleMemoryByteComputeUnitRatio)
}

func UpdateAutoScaleCpuNoAdjustUpperBoundRatioHandler(key string, value string, fs *pflag.FlagSet) {
	newVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("fail to parse config %v=%v, err: %v", key, value, err)
		return
	}
	if newVal < AutoScaleCpuRatio {
		log.Errorf("AutoScaleCpuNoAdjustUpperBoundRatio %v should be greater than AutoScaleCpuRatio %v, or may lead:"+
			" the CPU usage has exceeded the upper bound, but the newly allocated CPU resources may be less than the originally assigned amount.", newVal, AutoScaleCpuRatio)
		return
	}

	if err := fs.Set(key, value); err != nil {
		log.Errorf("fail to set config %v=%v, err: %v", key, value, err)
	}
}

func UpdateAutoScaleCpuNoAdjustLowerBoundRatioHandler(key string, value string, fs *pflag.FlagSet) {
	newVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("fail to parse config %v=%v, err: %v", key, value, err)
		return
	}
	if newVal > AutoScaleCpuRatio {
		log.Errorf("AutoScaleCpuNoAdjustLowerBoundRatio %v should be less than AutoScaleCpuRatio %v, or may lead: "+
			"the CPU usage is below the lower bound, but the newly allocated CPU resources may be greater than the originally assigned amount.", newVal, AutoScaleCpuRatio)
		return
	}

	if err := fs.Set(key, value); err != nil {
		log.Errorf("fail to set config %v=%v, err: %v", key, value, err)
	}
}

func UpdateAutoScaleMemoryNoAdjustUpperBoundRatioHandler(key string, value string, fs *pflag.FlagSet) {
	newVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("fail to parse config %v=%v, err: %v", key, value, err)
		return
	}
	if newVal < AutoScaleMemoryRatio {
		log.Errorf("AutoScaleMemoryNoAdjustUpperBoundRatio %v should be greater than AutoScaleMemoryRatio %v, or may lead:"+
			" the memory usage has exceeded the upper bound, but the newly allocated memory resources may be less than the originally assigned amount.", newVal, AutoScaleMemoryRatio)
		return
	}

	if err := fs.Set(key, value); err != nil {
		log.Errorf("fail to set config %v=%v, err: %v", key, value, err)
	}
}

func UpdateAutoScaleMemoryNoAdjustLowerBoundRatioHandler(key string, value string, fs *pflag.FlagSet) {
	newVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Errorf("fail to parse config %v=%v, err: %v", key, value, err)
		return
	}
	if newVal > AutoScaleMemoryRatio {
		log.Errorf("AutoScaleMemoryNoAdjustLowerBoundRatio %v should be less than AutoScaleMemoryRatio %v, or may lead:"+
			" the memory usage is below the lower bound, but the newly allocated memory resources may be greater than the originally assigned amount.", newVal, AutoScaleMemoryRatio)
		return
	}

	if err := fs.Set(key, value); err != nil {
		log.Errorf("fail to set config %v=%v, err: %v", key, value, err)
	}
}
