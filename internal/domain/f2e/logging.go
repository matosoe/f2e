package f2e

import (
	"encoding/json"
	"fmt"
	"time"
)

// CurrentTimeMillis returns the current time in milliseconds since epoch
func CurrentTimeMillis() int64 {
	return time.Now().UnixNano() / 1_000_000
}

// CalculateTPS calculates transactions per second given count and duration in milliseconds
func CalculateTPS(recordCount int64, durationMillis int64) float64 {
	if durationMillis <= 0 {
		return 0
	}
	return float64(recordCount) / (float64(durationMillis) / 1000.0)
}

// MarshalSummary marshals a summary struct to formatted JSON string
func MarshalSummary(v interface{}) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// FormatDuration formats milliseconds into a human-readable string
func FormatDuration(millis int64) string {
	seconds := float64(millis) / 1000.0
	if millis < 1000 {
		return fmt.Sprintf("%dms", millis)
	}
	return fmt.Sprintf("%.3fs", seconds)
}
