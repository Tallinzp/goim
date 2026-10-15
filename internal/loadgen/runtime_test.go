package loadgen

import (
	"runtime"
	"testing"
	"time"
)

// ReadRuntime reports the Go runtime's own counters (run 27: whether GC at
// GOMEMLIMIT holds loadgen's CPU in the storm's tail).
func TestReadRuntimeReportsGCAndMemory(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	go func() { <-done }() // one goroutine besides the test's
	runtime.GC()
	time.Sleep(time.Millisecond) // a scheduling wait to count
	rt := ReadRuntime()
	if rt.Goroutines < 2 || rt.GCCycles < 1 || rt.StackBytes <= 0 || rt.HeapLiveBytes <= 0 ||
		rt.HeapGoalBytes <= 0 || rt.MemLimitBytes <= 0 || rt.MappedBytes < rt.StackBytes+rt.HeapLiveBytes {
		t.Fatalf("%+v", rt)
	}
	if rt.GCCPUSeconds <= 0 || rt.UserCPUSeconds <= 0 {
		t.Fatalf("cpu: %+v", rt)
	}
	if rt.SchedWaits < 1 || rt.SchedWaitsOver1ms > rt.SchedWaits || rt.SchedWaitsOver100ms > rt.SchedWaitsOver1ms ||
		rt.SchedWaitsOver1s > rt.SchedWaitsOver100ms {
		t.Fatalf("sched: %+v", rt)
	}
}
