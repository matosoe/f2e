package internal

import (
	"testing"
	"time"
)

func TestLocalRFC3339TimestampUsesProcessTimezone(t *testing.T) {
	original := time.Local
	time.Local = time.FixedZone("execution", -3*60*60)
	t.Cleanup(func() { time.Local = original })

	got := localRFC3339Timestamp("2026-09-18T15:30:45.123456789Z")
	if want := "2026-09-18T12:30:45.123456789-03:00"; got != want {
		t.Fatalf("localRFC3339Timestamp() = %q, want %q", got, want)
	}
}

func TestLocalizeReportJSONUsesProcessTimezone(t *testing.T) {
	original := time.Local
	time.Local = time.FixedZone("execution", -3*60*60)
	t.Cleanup(func() { time.Local = original })

	got := localizeReportJSON(`{"metadata":{"createdAt":"2026-09-18T15:30:45Z"},"data":{"raw":"2026-09-18T15:30:45Z"}}`)
	want := `{"data":{"raw":"2026-09-18T15:30:45Z"},"metadata":{"createdAt":"2026-09-18T12:30:45-03:00"}}`
	if got != want {
		t.Fatalf("localizeReportJSON() = %s, want %s", got, want)
	}
}

func TestLocalRFC3339TimestampPreservesInvalidValue(t *testing.T) {
	if got := localRFC3339Timestamp("not-a-timestamp"); got != "not-a-timestamp" {
		t.Fatalf("localRFC3339Timestamp() = %q, want original value", got)
	}
}
