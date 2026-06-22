//go:build !linux

package metrics

import (
	"log"
	"runtime"
	"sync"
)

var cpuWarnOnce sync.Once

// ReadCPUTime returns 0 on non-Linux platforms. CPU time measurement
// without cgo/external deps is platform-specific; this fallback compiles
// and runs cleanly everywhere but reports unavailable data.
func ReadCPUTime() float64 {
	cpuWarnOnce.Do(func() {
		log.Printf("WARNING: process_cpu_seconds_total is not available on this platform (darwin/windows); returning 0")
	})
	return 0
}

// ReadResidentMemory returns the resident memory size using runtime.MemStats.Sys.
func ReadResidentMemory() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Sys)
}

// ReadGCStats returns the number of GC cycles completed.
func ReadGCStats() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return uint64(m.NumGC)
}
