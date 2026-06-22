//go:build linux

package broker

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

var lastProcCPUSeconds atomic.Int64

// readCPUTimeDelta returns the CPU seconds consumed since the last call.
// On Linux it reads /proc/self/stat for utime+stime.
func readCPUTimeDelta() float64 {
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

	// Convert clock ticks to nanoseconds (100 ticks/sec on most Linux).
	totalTicks := utime + stime
	totalNano := totalTicks * 10_000_000 // 1 tick = 10ms = 10_000_000 ns

	prev := lastProcCPUSeconds.Swap(totalNano)
	if prev == 0 {
		return 0 // first call, no delta
	}

	deltaNano := totalNano - prev
	if deltaNano < 0 {
		return 0
	}
	return float64(deltaNano) / 1e9
}
