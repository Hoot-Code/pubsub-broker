//go:build linux

package metrics

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// ReadCPUTime returns the cumulative user+system CPU time for this process.
// On Linux it reads /proc/self/stat which provides utime+stime in clock ticks.
// The result is converted to seconds using the standard 100 Hz tick rate.
func ReadCPUTime() float64 {
	f, err := os.Open("/proc/self/stat")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return 0
	}
	fields := strings.Fields(scanner.Text())
	// Field indices: 14=utime, 15=stime (1-based in the proc stat format).
	if len(fields) < 16 {
		return 0
	}
	utime, err := strconv.ParseInt(fields[13], 10, 64)
	if err != nil {
		return 0
	}
	stime, err := strconv.ParseInt(fields[14], 10, 64)
	if err != nil {
		return 0
	}

	// Most Linux systems run at 100 Hz (HZ=100).
	const ticksPerSec = 100
	return float64(utime+stime) / float64(ticksPerSec)
}

// ReadResidentMemory returns the current resident memory usage in bytes.
// On Linux it reads /proc/self/status for VmRSS. Falls back to runtime.MemStats.
func ReadResidentMemory() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return readResidentMemoryFallback()
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return float64(kb * 1024) // convert KB to bytes
				}
			}
			break
		}
	}
	return readResidentMemoryFallback()
}

func readResidentMemoryFallback() float64 {
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
