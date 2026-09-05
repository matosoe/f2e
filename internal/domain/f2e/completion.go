package f2e

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// CompletionEventVersion is the schema/version field for completion events.
// Consumers must check this field before deserializing; a change increments
// the minor version for backward-compatible additions, major version for
// breaking changes.
const CompletionEventVersion = "f2e-completion/1"

// CompletionEvent is the technical conclusion event published to the
// completion queue when a job reaches a terminal state (COMPLETED, FAILED or
// REJECTED). It is distinct from the record-level envelope queue.
//
// Consumers of this event must not treat it as a barrier between the record
// envelopes: the completion event may arrive before all record envelopes have
// been consumed from the record queue.
//
// Schema: documentacao/schemas/completion-v1.schema.json
type CompletionEvent struct {
	// SchemaVersion identifies the contract version. Consumers must check this
	// field and reject unknown versions to avoid silent misinterpretation.
	SchemaVersion string `json:"schemaVersion"`

	// EventID is a deterministic, content-addressed identifier derived from
	// jobId and completionVersion. Duplicate deliveries share the same eventId
	// so idempotent consumers can deduplicate them safely.
	EventID string `json:"eventId"`

	// JobID is the stable job identifier (= fileId for normal executions).
	JobID string `json:"jobId"`

	// FileID is the immutable physical-file identity.
	FileID string `json:"fileId"`

	// PrefixID identifies the configuration prefix that governed this job.
	PrefixID string `json:"prefixId,omitempty"`

	// Source carries the immutable S3 coordinates used during processing.
	Source CompletionSource `json:"source"`

	// Config carries enough provenance to correlate with the SSM version used.
	Config CompletionConfig `json:"config"`

	// Status is the terminal job status (COMPLETED, FAILED, REJECTED).
	Status JobStatus `json:"status"`

	// Result qualifies a COMPLETED job (SUCCESS or WITH_REJECTIONS).
	// Empty for FAILED and REJECTED.
	Result JobResult `json:"result,omitempty"`

	// Counts contains the aggregated record counters. CountsComplete is false
	// when a failed file could not produce a proven final total.
	Counts CompletionCounts `json:"counts"`

	// RejectionReason is the primary rejection reason for REJECTED jobs.
	// It is selected from the bounded catalog in states.go.
	RejectionReason RejectionReason `json:"rejectionReason,omitempty"`

	// Timestamps captures the significant instants of the job lifecycle.
	Timestamps CompletionTimestamps `json:"timestamps"`
}

// CompletionSource carries the immutable S3 coordinates used during processing.
type CompletionSource struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"versionId,omitempty"`
	ETag      string `json:"etag,omitempty"`
	FileSize  int64  `json:"fileSize,omitempty"`
}

// CompletionConfig carries the minimum provenance needed to correlate with the
// SSM version used during the job. Personal data and full configuration are not
// embedded in the event; the configHash can be used to look up the ledger.
type CompletionConfig struct {
	// ConfigID is a stable reference to the exact configuration snapshot (hash
	// of the SSM content), usable for ledger lookups.
	ConfigID string `json:"configId,omitempty"`
	// ParameterName is the SSM parameter that supplied the configuration.
	ParameterName string `json:"parameterName,omitempty"`
	// ParameterVersion is the SSM parameter version.
	ParameterVersion int64 `json:"parameterVersion,omitempty"`
}

// CompletionCounts mirrors Counts but uses explicit JSON field names suitable
// for the public event contract. RecordsRead = RecordsPublished + RecordsRejected
// + RecordsIgnored when CountsComplete is true.
type CompletionCounts struct {
	RecordsRead      int64 `json:"recordsRead"`
	RecordsPublished int64 `json:"recordsPublished"`
	RecordsRejected  int64 `json:"recordsRejected"`
	RecordsIgnored   int64 `json:"recordsIgnored"`
	// CountsComplete is false when the job failed partway and the final total
	// cannot be proven. Consumers must not assume completeness when false.
	CountsComplete bool `json:"countsComplete"`
}

// CompletionTimestamps records the significant lifecycle instants.
type CompletionTimestamps struct {
	ReceivedAt  *time.Time `json:"receivedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt"`
}

// CompletionIntent is the outbox record written atomically to the DynamoDB job
// item when a terminal state transition occurs. A separate publisher (T13)
// reads these via DynamoDB Streams or a recovery index and sends the event to
// the completion queue.
type CompletionIntent struct {
	// JobID identifies the job this intent belongs to. The DynamoDB item lives
	// under the same partition key as the job (JOB#<jobId>).
	JobID string `json:"jobId"`

	// Version is a monotonically increasing counter that allows a concurrent
	// terminal transition (crash + retry) to produce exactly one logical
	// intent. The EventID is derived from jobId + version so duplicate
	// deliveries share the same eventId.
	Version int64 `json:"version"`

	// Status is the terminal job status written at the time the intent was
	// recorded. It must not be changed after the fact.
	Status JobStatus `json:"status"`

	// Event is the full completion event payload, serialized when the intent
	// is written so the publisher does not need to reconstruct it.
	Event CompletionEvent `json:"event"`

	// CreatedAt is the instant the intent was written.
	CreatedAt time.Time `json:"createdAt"`

	// DeliveredAt is set by the publisher after SQS confirms the send.
	// A nil value means the intent has not been delivered yet.
	DeliveredAt *time.Time `json:"deliveredAt,omitempty"`
}

// CompletionEventID derives the deterministic event identifier from the jobId
// and the intent version. This ensures that a crash-and-retry scenario
// reproduces the same eventId and downstream consumers can deduplicate it.
func CompletionEventID(jobID string, version int64) string {
	raw := jobID + "/completion/" + itoa64(version)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func itoa64(n int64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for n > 0 {
		buf = append([]byte{digits[n%10]}, buf...)
		n /= 10
	}
	return string(buf)
}
