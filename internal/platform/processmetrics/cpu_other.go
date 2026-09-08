//go:build !linux

// Package processmetrics exposes lightweight process resource measurements.
package processmetrics

// CPUTime is unavailable on non-Linux development hosts. AWS Lambda runs the
// Linux implementation from cpu_linux.go.
func CPUTime() (userSeconds, systemSeconds float64, available bool) {
	return 0, 0, false
}
