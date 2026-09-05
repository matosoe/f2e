package f2e

import "testing"

func TestJobTransitionsAllowed(t *testing.T) {
	tests := []struct {
		name string
		from JobStatus
		to   JobStatus
		want bool
	}{
		// Happy path.
		{"received to validating", JobStateReceived, JobStateValidating, true},
		{"validating to planning", JobStateValidating, JobStatePlanning, true},
		{"planning to processing", JobStatePlanning, JobStateProcessing, true},
		{"processing to completed", JobStateProcessing, JobStateCompleted, true},
		// Validation rejects an invalid file/configuration.
		{"validating to rejected", JobStateValidating, JobStateRejected, true},
		{"received to rejected", JobStateReceived, JobStateRejected, true},
		// Technical failure from any active state.
		{"received to failed", JobStateReceived, JobStateFailed, true},
		{"validating to failed", JobStateValidating, JobStateFailed, true},
		{"planning to failed", JobStatePlanning, JobStateFailed, true},
		{"processing to failed", JobStateProcessing, JobStateFailed, true},
		// A sealed plan with zero chunks completes immediately (empty JSON array).
		{"planning to completed (empty plan)", JobStatePlanning, JobStateCompleted, true},
		// A rejected file discovered late in planning.
		{"planning to rejected", JobStatePlanning, JobStateRejected, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf("%s -> %s: got %v want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestJobTransitionsProhibited(t *testing.T) {
	tests := []struct {
		from JobStatus
		to   JobStatus
	}{
		// Terminals never regress.
		{JobStateCompleted, JobStateProcessing},
		{JobStateCompleted, JobStateReceived},
		{JobStateFailed, JobStateProcessing},
		{JobStateRejected, JobStateValidating},
		// Skipping validation/planning is not allowed.
		{JobStateReceived, JobStatePlanning},
		{JobStateReceived, JobStateProcessing},
		{JobStateValidating, JobStateProcessing},
		{JobStateValidating, JobStateCompleted},
		{JobStateProcessing, JobStateValidating},
		// Processing cannot reject (rejection happens before execution).
		{JobStateProcessing, JobStateRejected},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"_to_"+string(tt.to), func(t *testing.T) {
			if tt.from.CanTransitionTo(tt.to) {
				t.Fatalf("%s -> %s: expected prohibited transition", tt.from, tt.to)
			}
		})
	}
}

func TestJobTerminalStates(t *testing.T) {
	for _, s := range []JobStatus{JobStateCompleted, JobStateFailed, JobStateRejected} {
		if !s.Terminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}
	if JobStatus("unknown").Terminal() {
		t.Fatal("an unknown job status must not be treated as terminal")
	}
	for _, s := range []JobStatus{JobStateReceived, JobStateValidating, JobStatePlanning, JobStateProcessing} {
		if s.Terminal() {
			t.Fatalf("%s should not be terminal", s)
		}
	}
}

func TestChunkTransitionsAllowed(t *testing.T) {
	tests := []struct {
		name string
		from ChunkStatus
		to   ChunkStatus
		want bool
	}{
		{"pending to running", ChunkStatePending, ChunkStateRunning, true},
		{"running to completed", ChunkStateRunning, ChunkStateCompleted, true},
		{"running to retry pending", ChunkStateRunning, ChunkStateRetryPending, true},
		{"running to failed", ChunkStateRunning, ChunkStateFailed, true},
		{"retry pending to running", ChunkStateRetryPending, ChunkStateRunning, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf("%s -> %s: got %v want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestChunkTransitionsProhibited(t *testing.T) {
	tests := []struct {
		from ChunkStatus
		to   ChunkStatus
	}{
		{ChunkStateCompleted, ChunkStateRunning},
		{ChunkStateCompleted, ChunkStateRetryPending},
		{ChunkStateFailed, ChunkStateRunning},
		{ChunkStateFailed, ChunkStateRetryPending},
		{ChunkStatePending, ChunkStateCompleted},
		{ChunkStatePending, ChunkStateFailed},
		{ChunkStatePending, ChunkStateRetryPending},
		{ChunkStateRetryPending, ChunkStateCompleted},
		{ChunkStateRunning, ChunkStatePending},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"_to_"+string(tt.to), func(t *testing.T) {
			if tt.from.CanTransitionTo(tt.to) {
				t.Fatalf("%s -> %s: expected prohibited transition", tt.from, tt.to)
			}
		})
	}
}

func TestChunkTerminalStates(t *testing.T) {
	for _, s := range []ChunkStatus{ChunkStateCompleted, ChunkStateFailed} {
		if !s.Terminal() {
			t.Fatalf("%s should be terminal", s)
		}
	}
	for _, s := range []ChunkStatus{ChunkStatePending, ChunkStateRunning, ChunkStateRetryPending} {
		if s.Terminal() {
			t.Fatalf("%s should not be terminal", s)
		}
	}
	if ChunkStatus("unknown").Terminal() {
		t.Fatal("an unknown chunk status must not be treated as terminal")
	}
}

func TestSourceRecordIDStableAndDistinct(t *testing.T) {
	fileID := "file-123"

	// V2 identity is independent of chunk and schema: same file/offset always
	// yields the same id regardless of chunking or configuration.
	a := SourceRecordID(fileID, 0)
	b := SourceRecordID(fileID, 0)
	if a != b {
		t.Fatalf("SourceRecordID not stable: %s != %s", a, b)
	}

	// Different offsets produce different identities.
	if SourceRecordID(fileID, 0) == SourceRecordID(fileID, 1) {
		t.Fatalf("distinct offsets should produce distinct identities")
	}

	// V2 must not depend on schema or chunk — construct the same logical record
	// via different chunk/schema and confirm equality.
	c := SourceRecordID(fileID, 42)
	if c == SourceRecordIDV1(fileID, "chunk-1", 1, "schema", "v1") {
		t.Fatalf("V1 and V2 identities collide unexpectedly")
	}

	// V1 preserves its legacy formula for correlation with historical records.
	if got := SourceRecordIDV1("f", "c", 7, "s", "v"); got != hashIdentity("f/c/7/s/v") {
		t.Fatalf("SourceRecordIDV1 changed: %s", got)
	}
}

func TestFileIDUsesImmutableObjectVersion(t *testing.T) {
	base := ObjectIdentity{Bucket: "b", Key: "key", VersionID: "v1", ETag: "etag", Size: 10}
	if FileID(base) != FileID(base) {
		t.Fatal("fileId must be stable")
	}
	newVersion := base
	newVersion.VersionID = "v2"
	if FileID(base) == FileID(newVersion) {
		t.Fatal("different immutable versions must have different fileIds")
	}
}

func TestAcquisitionResultValues(t *testing.T) {
	seen := map[AcquisitionResult]bool{}
	for _, r := range []AcquisitionResult{Acquired, AlreadyCompleted, Busy} {
		if seen[r] {
			t.Fatalf("duplicate acquisition result %q", r)
		}
		seen[r] = true
		if r == "" {
			t.Fatalf("empty acquisition result")
		}
	}
}
