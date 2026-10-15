package loadgen

import (
	"runtime/metrics"
	"time"
)

// Runtime is loadgen's own Go runtime counters now: GET /runtime (run 27,
// a diagnostic). The CPU seconds and the scheduling waits are cumulative
// since loadgen started: a series of samples gives their rate. The CPU
// seconds are the runtime's estimates, comparable only with each other
// (GCCPUSeconds against UserCPUSeconds), not with the OS's. MappedBytes is
// what the runtime holds from the OS (total less released heap), the
// figure GOMEMLIMIT bounds; StackBytes is goroutine stacks in it.
type Runtime struct {
	GCCPUSeconds        float64 `json:"gc_cpu_s"`   // GC: background workers, assists, pauses
	UserCPUSeconds      float64 `json:"user_cpu_s"` // Go code outside the runtime
	GCCycles            uint64  `json:"gc_cycles"`
	GCLimiterLastCycle  uint64  `json:"gc_limiter_last_cycle"` // the latest cycle the GC CPU limiter capped (0: never)
	Goroutines          uint64  `json:"goroutines"`
	StackBytes          uint64  `json:"stack_bytes"`
	HeapLiveBytes       uint64  `json:"heap_live_bytes"` // marked live by the latest GC
	HeapGoalBytes       uint64  `json:"heap_goal_bytes"`
	MappedBytes         uint64  `json:"mapped_bytes"`
	MemLimitBytes       int64   `json:"mem_limit_bytes"`
	SchedWaits          uint64  `json:"sched_waits"` // goroutines made runnable, then run
	SchedWaitsOver1ms   uint64  `json:"sched_waits_over_1ms"`
	SchedWaitsOver100ms uint64  `json:"sched_waits_over_100ms"`
	SchedWaitsOver1s    uint64  `json:"sched_waits_over_1s"`
}

var runtimeMetrics = []string{
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/user:cpu-seconds",
	"/gc/cycles/total:gc-cycles",
	"/gc/limiter/last-enabled:gc-cycle",
	"/sched/goroutines:goroutines",
	"/memory/classes/heap/stacks:bytes",
	"/memory/classes/os-stacks:bytes",
	"/gc/heap/live:bytes",
	"/gc/heap/goal:bytes",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
	"/gc/gomemlimit:bytes",
	"/sched/latencies:seconds",
}

// ReadRuntime reads Runtime from runtime/metrics.
func ReadRuntime() Runtime {
	s := make([]metrics.Sample, len(runtimeMetrics))
	for i, name := range runtimeMetrics {
		s[i].Name = name
	}
	metrics.Read(s)
	u := func(i int) uint64 {
		if s[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return s[i].Value.Uint64()
	}
	f := func(i int) float64 {
		if s[i].Value.Kind() != metrics.KindFloat64 {
			return 0
		}
		return s[i].Value.Float64()
	}
	rt := Runtime{GCCPUSeconds: f(0), UserCPUSeconds: f(1), GCCycles: u(2), GCLimiterLastCycle: u(3),
		Goroutines: u(4), StackBytes: u(5) + u(6), HeapLiveBytes: u(7), HeapGoalBytes: u(8),
		MappedBytes: u(9) - u(10), MemLimitBytes: int64(u(11))}
	if s[12].Value.Kind() == metrics.KindFloat64Histogram {
		h := s[12].Value.Float64Histogram()
		// Buckets[i] is bucket i's lower bound: a bucket counts over a
		// threshold when it starts at or above it.
		for i, n := range h.Counts {
			rt.SchedWaits += n
			lo := h.Buckets[i]
			if lo >= time.Millisecond.Seconds() {
				rt.SchedWaitsOver1ms += n
			}
			if lo >= (100 * time.Millisecond).Seconds() {
				rt.SchedWaitsOver100ms += n
			}
			if lo >= time.Second.Seconds() {
				rt.SchedWaitsOver1s += n
			}
		}
	}
	return rt
}
