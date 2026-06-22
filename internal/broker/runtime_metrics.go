package broker

import (
	"runtime"
	"sync/atomic"
)

// lastGCCount stores the previous GC cycle count for delta computation.
var lastGCCount atomic.Uint64

// readResidentMemory returns the current resident memory size in bytes.
// Uses runtime.MemStats.Sys as a cross-platform approximation.
func readResidentMemory() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Sys)
}

// readGCCountDelta returns the number of new GC cycles since the last call.
// Returns 0 on the first call (baseline).
func readGCCountDelta() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	cur := uint64(m.NumGC)
	prev := lastGCCount.Swap(cur)
	if cur > prev {
		return cur - prev
	}
	return 0
}
