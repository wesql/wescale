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
	FilterQPSRates              *stats.Rates
	FilterWasmMemorySize        *stats.CountersWithMultiLabels
}

func NewActionStats(exporter *servenv.Exporter) *ActionStats {
	stats := &ActionStats{
		FilterBeforeExecutionTiming: exporter.NewTimings("FilterBeforeExecution", "Filter before execution timings", "Name"),
		FilterAfterExecutionTiming:  exporter.NewTimings("FilterAfterExecution", "Filter before execution timings", "Name"),
		FilterErrorCounts:           exporter.NewCountersWithSingleLabel("FilterErrorCounts", "filter error counts", "Name"),
		FilterWasmMemorySize:        exporter.NewCountersWithMultiLabels("FilterWasmMemorySize", "Wasm memory size", []string{"Name", "BeforeOrAfter"}),
	}
	stats.FilterQPSRates = exporter.NewRates("FilterQps", stats.FilterBeforeExecutionTiming, 15*60/5, 5*time.Second)
	return stats
}
