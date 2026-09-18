// Package runtimeclock configures the process-local timezone when a runtime
// supplies an explicit offset (for example, LocalStack started by the E2E scripts).
package runtimeclock

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ConfigureLocalTimezone applies F2E_TIMEZONE_OFFSET when it is a valid
// RFC3339 offset such as -03:00. Without it, time.Local remains the timezone
// configured by the operating system.
func ConfigureLocalTimezone() error {
	offset := os.Getenv("F2E_TIMEZONE_OFFSET")
	if offset == "" {
		return nil
	}
	if len(offset) != 6 || (offset[0] != '+' && offset[0] != '-') || offset[3] != ':' {
		return fmt.Errorf("F2E_TIMEZONE_OFFSET must use ±HH:MM, got %q", offset)
	}
	hours, hourErr := strconv.Atoi(offset[1:3])
	minutes, minuteErr := strconv.Atoi(offset[4:6])
	if hourErr != nil || minuteErr != nil || hours > 14 || minutes > 59 {
		return fmt.Errorf("invalid F2E_TIMEZONE_OFFSET %q", offset)
	}
	seconds := hours*60*60 + minutes*60
	if offset[0] == '-' {
		seconds = -seconds
	}
	time.Local = time.FixedZone(offset, seconds)
	return nil
}
