//go:build linux

// Package processmetrics exposes lightweight process resource measurements.
package processmetrics

import "syscall"

// CPUTime returns cumulative user and system CPU time consumed by this process.
func CPUTime() (userSeconds, systemSeconds float64, available bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0, false
	}
	return timevalSeconds(usage.Utime), timevalSeconds(usage.Stime), true
}

func timevalSeconds(value syscall.Timeval) float64 {
	return float64(value.Sec) + float64(value.Usec)/1_000_000
}
