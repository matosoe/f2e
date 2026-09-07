// Package f2e defines the domain model for the F2E pipeline state machine.
//
// This file formalizes the identities, states and transitions used by the
// pipeline.
// The canonical job/chunk states below coexist with the legacy JobStatus
// constants in ledger.go; the latter are kept only for backward compatibility
// with the existing DynamoDB ledger and are superseded as persistence migrates
// (T08 onward).
package f2e

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

// SourceRecordID returns the stable, immutable identity of a single logical
// record. It depends only on the physical source (fileId) and the record's
// starting physical byte offset, so it is independent of chunking, SQS bundle
// grouping and schema/configuration version (which are kept as separate
// metadata). It is versioned as "v2" relative to the legacy formula in
// SourceRecordIDV1.
func SourceRecordID(fileID string, byteOffset int64) string {
	return hashIdentity(fileID + "/" + strconv.FormatInt(byteOffset, 10))
}

// SourceRecordIDV1 preserves the legacy identity formula for compatibility and
// audit of records produced before the identity was decoupled from chunking and
// schema. It is kept so consumers can correlate old sourceRecordId values with
// the new ones; new production must use SourceRecordID.
func SourceRecordIDV1(fileID, chunkID string, recordNumber int64, schemaID, schemaVersion string) string {
	return hashIdentity(fileID + "/" + chunkID + "/" + strconv.FormatInt(recordNumber, 10) + "/" + schemaID + "/" + schemaVersion)
}

func hashIdentity(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// Job state machine
// ---------------------------------------------------------------------------

// JobStatus (declared in ledger.go) is the lifecycle state of an aggregated job.
// The canonical flow is:
//
//	RECEIVED -> VALIDATING -> PLANNING -> PROCESSING -> COMPLETED
//	                    |             |           |
//	                    +-------------+-----------+--> FAILED
//	                    |
//	                    +--> REJECTED
//
// The legacy constants (JobPending, JobScheduled, etc.) in ledger.go remain
// only for compatibility with the current DynamoDB ledger and are superseded by
// these canonical states.
const (
	JobStateReceived   JobStatus = "RECEIVED"
	JobStateValidating JobStatus = "VALIDATING"
	JobStatePlanning   JobStatus = "PLANNING"
	JobStateProcessing JobStatus = "PROCESSING"
	JobStateCompleted  JobStatus = "COMPLETED"
	JobStateFailed     JobStatus = "FAILED"
	JobStateRejected   JobStatus = "REJECTED"
)

// JobResult qualifies a terminal COMPLETED job. A job may finish technically
// complete while still rejecting some individual records.
type JobResult string

const (
	JobResultSuccess        JobResult = "SUCCESS"
	JobResultWithRejections JobResult = "WITH_REJECTIONS"
)

// jobTransitions is the authoritative table of valid job transitions. Terminal
// states (COMPLETED, FAILED, REJECTED) have no outgoing edges and never regress.
var jobTransitions = map[JobStatus][]JobStatus{
	JobStateReceived:   {JobStateValidating, JobStateRejected, JobStateFailed},
	JobStateValidating: {JobStatePlanning, JobStateRejected, JobStateFailed},
	// A sealed plan with zero chunks (e.g. empty JSON array) may go straight to
	// COMPLETED; a rejected file discovered during planning may be REJECTED.
	JobStatePlanning:   {JobStateProcessing, JobStateCompleted, JobStateRejected, JobStateFailed},
	JobStateProcessing: {JobStateCompleted, JobStateFailed},
	JobStateCompleted:  nil,
	JobStateFailed:     nil,
	JobStateRejected:   nil,
}

// CanTransitionTo reports whether moving from s to next is allowed.
func (s JobStatus) CanTransitionTo(next JobStatus) bool {
	for _, allowed := range jobTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Terminal reports whether s is a terminal state that cannot regress.
func (s JobStatus) Terminal() bool {
	transitions, known := jobTransitions[s]
	return known && len(transitions) == 0
}

// ---------------------------------------------------------------------------
// Chunk state machine
// ---------------------------------------------------------------------------

// ChunkStatus is the lifecycle state of a single chunk:
//
//	PENDING -> RUNNING -> COMPLETED
//	                |
//	                +--> RETRY_PENDING -> RUNNING -> ...
//	                |
//	                +--> FAILED
type ChunkStatus string

const (
	ChunkStatePending      ChunkStatus = "PENDING"
	ChunkStateRunning      ChunkStatus = "RUNNING"
	ChunkStateRetryPending ChunkStatus = "RETRY_PENDING"
	ChunkStateCompleted    ChunkStatus = "COMPLETED"
	ChunkStateFailed       ChunkStatus = "FAILED"
)

// chunkTransitions is the authoritative table of valid chunk transitions.
var chunkTransitions = map[ChunkStatus][]ChunkStatus{
	ChunkStatePending:      {ChunkStateRunning},
	ChunkStateRunning:      {ChunkStateCompleted, ChunkStateRetryPending, ChunkStateFailed},
	ChunkStateRetryPending: {ChunkStateRunning},
	ChunkStateCompleted:    nil,
	ChunkStateFailed:       nil,
}

// CanTransitionTo reports whether moving from s to next is allowed.
func (s ChunkStatus) CanTransitionTo(next ChunkStatus) bool {
	for _, allowed := range chunkTransitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Terminal reports whether s is a terminal state that cannot regress.
func (s ChunkStatus) Terminal() bool {
	transitions, known := chunkTransitions[s]
	return known && len(transitions) == 0
}

// ---------------------------------------------------------------------------
// Acquisition result
// ---------------------------------------------------------------------------

// AcquisitionResult reports the outcome of conditionally acquiring a job or
// chunk for processing. It lets the caller distinguish a successful claim from
// the idempotent "already done" case and the contention "busy" case without
// conflating them with a technical error.
type AcquisitionResult string

const (
	Acquired         AcquisitionResult = "acquired"
	AlreadyCompleted AcquisitionResult = "alreadyCompleted"
	Busy             AcquisitionResult = "busy"
)

// ---------------------------------------------------------------------------
// Counts, reasons and transitions
// ---------------------------------------------------------------------------

// Counts aggregates the logical record counters of a job or chunk. With a
// sealed manifest the invariant
//
//	expectedChunks = pendingChunks + runningChunks + retryPendingChunks +
//	                 completedChunks + failedChunks
//
// holds; per completed chunk:
//
//	recordsRead = recordsPublished + recordsRejected + recordsIgnored.
//
// messagesPublished is a separate measure (it depends on single/bundle mode).
// Re-sends do not increase logical totals.
type Counts struct {
	RecordsRead          int64
	RecordsPublished     int64
	RecordsRejected      int64
	RecordsIgnored       int64
	MessagesPublished    int64
	PhysicalLinesIgnored int64
	Reasons              map[RejectionReason]int64
	// CountsComplete is false when a failed file cannot prove a final count and
	// only partial progress is known.
	CountsComplete bool
}

// RejectionReason classifies why a record or file was rejected. Record-level
// rejections are not infrastructure failures: a job may complete WITH_REJECTIONS.
type RejectionReason string

const (
	RejectionInvalidUTF8       RejectionReason = "INVALID_UTF8"
	RejectionUnterminatedLine  RejectionReason = "UNTERMINATED_LINE"
	RejectionOversizedRecord   RejectionReason = "OVERSIZED_RECORD"
	RejectionEmptyFile         RejectionReason = "EMPTY_FILE"
	RejectionInvalidConfig     RejectionReason = "INVALID_CONFIG"
	RejectionUnsupportedType   RejectionReason = "UNSUPPORTED_TYPE"
	RejectionProcessorRejected RejectionReason = "PROCESSOR_REJECTED"
	RejectionInvalidDecision   RejectionReason = "INVALID_DECISION"
	IgnoreProcessorFiltered    RejectionReason = "PROCESSOR_FILTERED"
	IgnoreMultiLineHeader      RejectionReason = "MULTILINE_HEADER"
	IgnoreMultiLineTrailer     RejectionReason = "MULTILINE_TRAILER"
)

// RecordDecisionKind is the explicit reconciliation outcome of one logical
// record. Technical errors remain Go errors and are retried; they are never
// silently converted into a record decision.
type RecordDecisionKind string

const (
	RecordPublish RecordDecisionKind = "publish"
	RecordReject  RecordDecisionKind = "reject"
	RecordIgnore  RecordDecisionKind = "ignore"
)

// RecordDecision keeps the decision deliberately free of arbitrary payload or
// personal-data dimensions. Reason is selected from the bounded catalog above.
type RecordDecision struct {
	Kind     RecordDecisionKind
	Envelope *Envelope[RecordPayload]
	Reason   RejectionReason
}

func PublishRecord(envelope Envelope[RecordPayload]) RecordDecision {
	return RecordDecision{Kind: RecordPublish, Envelope: &envelope}
}

// Rejection records a single rejection decision and its reason for
// reconciliation in the ledger.
type Rejection struct {
	RecordID string          `json:"recordId,omitempty"`
	Reason   RejectionReason `json:"reason"`
	Detail   string          `json:"detail,omitempty"`
}

// Transition records a single state change with its timestamp and, when
// applicable, the reason that motivated it.
type Transition struct {
	From   string    `json:"from"`
	To     string    `json:"to"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason,omitempty"`
}

// ---------------------------------------------------------------------------
// Aggregate views
// ---------------------------------------------------------------------------

// Receipt identifies a single intake occurrence (S3 notification or explicit
// request). A redelivery of the same notification reuses the same receiptId via
// the SQS messageId; a new physical version is a new receipt.
type Receipt struct {
	ReceiptID string         `json:"receiptId"`
	FileID    string         `json:"fileId"`
	Source    ObjectIdentity `json:"source"`
	// Environment disambiguates the deployment (e.g. dev/staging/prod) in the
	// admission identity.
	Environment    string                `json:"environment,omitempty"`
	ReceivedAt     time.Time             `json:"receivedAt"`
	ConfigSnapshot ConfigurationSnapshot `json:"configSnapshot,omitempty"`
	// PrefixID is the canonical identifier of the registered prefix (T20).
	// When set, the ledger persists it on the job item so quota release (T22)
	// can be performed without re-reading the SSM configuration.
	PrefixID string `json:"prefixId,omitempty"`
}

// Job is the aggregate view of one processing execution bound to a fileId.
type Job struct {
	JobID          string    `json:"jobId"`
	FileID         string    `json:"fileId"`
	ReceiptID      string    `json:"receiptId,omitempty"`
	Status         JobStatus `json:"status"`
	Result         JobResult `json:"result,omitempty"`
	ExpectedChunks int       `json:"expectedChunks"`
	Counts         Counts    `json:"counts"`

	ConfigSnapshot ConfigurationSnapshot `json:"configSnapshot,omitempty"`

	ReceivedAt  *time.Time `json:"receivedAt,omitempty"`
	PlannedAt   *time.Time `json:"plannedAt,omitempty"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	UpdatedAt   *time.Time `json:"updatedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// Chunk is the aggregate view of one chunk within a job.
type Chunk struct {
	ChunkID string      `json:"chunkId"`
	JobID   string      `json:"jobId"`
	Status  ChunkStatus `json:"status"`
	Attempt int         `json:"attempt"`
	Counts  Counts      `json:"counts"`

	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}
