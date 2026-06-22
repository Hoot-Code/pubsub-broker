//go:build !linux

package broker

import (
	"log"
	"sync"
)

var cpuWarnOnce sync.Once

// readCPUTimeDelta returns 0 on non-Linux platforms. CPU time measurement
// without cgo/external deps is platform-specific; this fallback compiles
// and runs cleanly everywhere but reports unavailable data.
func readCPUTimeDelta() float64 {
	cpuWarnOnce.Do(func() {
		log.Printf("WARNING: process_cpu_seconds_total is not available on this platform (darwin/windows); returning 0")
	})
	return 0
}
