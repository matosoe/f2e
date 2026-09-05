package f2e

import (
	"strings"
	"testing"
	"time"
)

func TestCompletionEventIDIsDeterministic(t *testing.T) {
	id1 := CompletionEventID("job-abc", 1)
	id2 := CompletionEventID("job-abc", 1)
	if id1 != id2 {
		t.Fatalf("CompletionEventID not deterministic: %q vs %q", id1, id2)
	}
	if len(id1) != 64 {
		t.Fatalf("CompletionEventID length=%d, want 64", len(id1))
	}
}

func TestCompletionEventIDDiffersAcrossJobsAndVersions(t *testing.T) {
	cases := [][2]string{
		{CompletionEventID("job-a", 1), CompletionEventID("job-b", 1)},
		{CompletionEventID("job-a", 1), CompletionEventID("job-a", 2)},
	}
	for _, pair := range cases {
		if pair[0] == pair[1] {
			t.Fatalf("collision between %q and %q", pair[0], pair[1])
		}
	}
}

func TestCompletionEventIDIsLowercaseHex(t *testing.T) {
	id := CompletionEventID("any-job", 0)
	for _, c := range id {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("CompletionEventID contains non-hex character: %q", c)
		}
	}
}

func TestCompletionIntentSchema(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(5 * time.Minute)
	event := CompletionEvent{
		SchemaVersion: CompletionEventVersion,
		EventID:       CompletionEventID("job-xyz", 1),
		JobID:         "job-xyz",
		FileID:        "file-xyz",
		Source: CompletionSource{
			Bucket:    "bucket",
			Key:       "key/file.txt",
			VersionID: "v1",
			ETag:      "etag",
			FileSize:  1024,
		},
		Config: CompletionConfig{
			ConfigID:         "cfghash",
			ParameterName:    "/f2e/prod/prefix",
			ParameterVersion: 2,
		},
		Status: JobStateCompleted,
		Result: JobResultSuccess,
		Counts: CompletionCounts{
			RecordsRead:      100,
			RecordsPublished: 100,
			CountsComplete:   true,
		},
		Timestamps: CompletionTimestamps{
			ReceivedAt:  &now,
			CompletedAt: &completedAt,
		},
	}
	if event.SchemaVersion != CompletionEventVersion {
		t.Errorf("schemaVersion=%q, want %q", event.SchemaVersion, CompletionEventVersion)
	}
	if len(event.EventID) != 64 {
		t.Errorf("eventId length=%d, want 64", len(event.EventID))
	}
	if event.Status != JobStateCompleted {
		t.Errorf("status=%q, want COMPLETED", event.Status)
	}
	if event.Result != JobResultSuccess {
		t.Errorf("result=%q, want SUCCESS", event.Result)
	}
	if !event.Counts.CountsComplete {
		t.Error("countsComplete should be true")
	}
	if event.Counts.RecordsRead != event.Counts.RecordsPublished+event.Counts.RecordsRejected+event.Counts.RecordsIgnored {
		t.Error("counts invariant violated: recordsRead != published + rejected + ignored")
	}
}

func TestCompletionIntentVersionZeroProducesValidID(t *testing.T) {
	id := CompletionEventID("job", 0)
	if len(id) != 64 {
		t.Fatalf("zero-version eventId length=%d, want 64", len(id))
	}
}

// TestTerminalStatusesHaveCompletionEvents documents which terminal job states
// must produce a completion event. COMPLETED, FAILED and REJECTED are all
// terminal; no other states should emit a completion event.
func TestTerminalStatusesHaveCompletionEvents(t *testing.T) {
	terminalWithEvent := []JobStatus{
		JobStateCompleted,
		JobStateFailed,
		JobStateRejected,
	}
	for _, s := range terminalWithEvent {
		if !s.Terminal() {
			t.Errorf("status %q is expected to be terminal but Terminal() returned false", s)
		}
	}
	// Non-terminal states must not produce completion events.
	nonTerminal := []JobStatus{
		JobStateReceived,
		JobStateValidating,
		JobStatePlanning,
		JobStateProcessing,
	}
	for _, s := range nonTerminal {
		if s.Terminal() {
			t.Errorf("status %q should not be terminal", s)
		}
	}
}
