package f2e

import "time"

type JobStatus string

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
}
