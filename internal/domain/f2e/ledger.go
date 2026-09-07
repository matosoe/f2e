package f2e

import "time"

type JobStatus string

// Legacy job status constants. These predate the canonical state machine in
// states.go and are retained only so the current DynamoDB ledger and
// its callers keep compiling without semantic change. They are superseded by
// JobStateReceived/Validating/Planning/Processing/Completed/Failed/Rejected.
//
// Mapping for reference (compatibility is explicit, not accidental):
//
//	JobPending          -> JobStatePlanning   (plan sealed, chunks pending)
//	JobScheduled        -> JobStateProcessing (chunks queued for execution)
//	JobSchedulingFailed -> JobStateFailed
//	JobRunning          -> JobStateProcessing
//	JobCompleted        -> JobStateCompleted
//	JobFailed           -> JobStateFailed
const (
	JobPending          JobStatus = "PENDING"
	JobScheduled        JobStatus = "SCHEDULED"
	JobSchedulingFailed JobStatus = "SCHEDULING_FAILED"
	JobRunning          JobStatus = "RUNNING"
	JobCompleted        JobStatus = "COMPLETED"
	JobFailed           JobStatus = "FAILED"
)

type JobPlan struct {
	JobID          string
	FileID         string
	Bucket         string
	Key            string
	VersionID      string
	ETag           string
	ExpectedChunks int
	CreatedAt      time.Time
	ConfigSnapshot ConfigurationSnapshot
}

type ChunkResult struct {
	JobID           string
	ChunkID         string
	Attempt         int
	RecordsProduced int64
	BytesProcessed  int64
	Error           string
	OccurredAt      time.Time
	Counts          Counts
	Reasons         map[RejectionReason]int64
}
