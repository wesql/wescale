package tabletserver

import (
	"time"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
)

type ActionStats struct {
	FilterBeforeExecutionTiming *servenv.TimingsWrapper
	FilterAfterExecutionTiming  *servenv.TimingsWrapper
	FilterErrorCounts           *stats.CountersWithSingleLabel
	QPSRates                    *stats.Rates
	WasmMemorySize              *stats.CountersWithMultiLabels
}

func NewActionStats(exporter *servenv.Exporter) *ActionStats {
	stats := &ActionStats{
		FilterBeforeExecutionTiming: exporter.NewTimings("FilterBeforeExecution", "Filter before execution timings", "Name"),
		FilterAfterExecutionTiming:  exporter.NewTimings("FilterAfterExecution", "Filter before execution timings", "Name"),
		FilterErrorCounts:           exporter.NewCountersWithSingleLabel("FilterErrorCounts", "filter error counts", "Name"),
		WasmMemorySize:              exporter.NewCountersWithMultiLabels("WasmMemorySize", "Wasm memory size", []string{"Name", "BeforeOrAfter"}),
	}
	stats.QPSRates = exporter.NewRates("FilterQps", stats.FilterBeforeExecutionTiming, 15*60/5, 5*time.Second)
	return stats
}
