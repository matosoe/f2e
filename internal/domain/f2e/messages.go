// Package f2e defines the messages exchanged by the F2E pipeline.
package f2e

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
)

const SchemaVersion = "1"

type FileReference struct {
	Bucket    string
	Key       string
	VersionID string
}

type ObjectIdentity struct {
	Bucket    string
	Key       string
	VersionID string
	ETag      string
	Size      int64
}

// DataType describes how the object is split and represented by workers.
type DataType string

const (
	DataTypeText      DataType = "text"
	DataTypeMultiLine DataType = "multi-line"
	DataTypeJSON      DataType = "json"
)

// MultiLineLayout describes how to group physical lines into logical multi-line records.
type MultiLineLayout struct {
	// BreakPosition is the 0-based byte offset within a line to check for BreakMarker.
	BreakPosition int `json:"breakPosition,omitempty"`
	// BreakMarker is the string that, when found at BreakPosition, signals the start of a new record.
	BreakMarker string `json:"breakMarker"`
	// AcceptedPrefixes are strings (checked from BreakPosition) that indicate a line belongs to the current record.
	// Lines matching neither BreakMarker nor any AcceptedPrefix are skipped (headers/trailers).
	// When empty, all lines after a break line are included.
	AcceptedPrefixes []string `json:"acceptedPrefixes,omitempty"`
	// LineSeparator joins lines within a record. Defaults to "\x1C" (ASCII file separator).
	LineSeparator string `json:"lineSeparator,omitempty"`
	// MaxBytesPerRecord is the maximum total byte size of one complete record (all its lines including newlines).
	// Required for chunk boundary planning.
	MaxBytesPerRecord int64 `json:"maxBytesPerRecord,omitempty"`
}

// JSONArrayLayout describes how to locate and iterate an array inside a JSON file.
type JSONArrayLayout struct {
	// ArrayPath is the dot-separated key path to the target array (empty = root array).
	ArrayPath string `json:"arrayPath,omitempty"`
	// MaxBytesPerElement is the maximum byte size of one array element; required for chunk planning.
	MaxBytesPerElement int64 `json:"maxBytesPerElement"`
}

// CorporateContext carries trace identifiers through every internal hop and
// into the self-contained output envelope.
type CorporateContext struct {
	TransactionID string `json:"transactionId,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
	TraceID       string `json:"traceId,omitempty"`
	SourceSystem  string `json:"sourceSystem,omitempty"`
}

type FileRequest struct {
	Bucket               string           `json:"bucket"`
	Key                  string           `json:"key"`
	VersionID            string           `json:"versionId,omitempty"`
	PresignedURL         string           `json:"presignedUrl,omitempty"`
	DataType             DataType         `json:"dataType"`
	MaxRecordLengthBytes int64            `json:"maxRecordLengthBytes,omitempty"`
	MultiLineLayout      MultiLineLayout  `json:"multiLineLayout,omitempty"`
	JSONArrayLayout      JSONArrayLayout  `json:"jsonArrayLayout,omitempty"`
	Context              CorporateContext `json:"context,omitempty"`
}

// FileID identifies one immutable physical object version. Configuration and
// receipt identity are intentionally excluded so retries cannot create a
// second normal execution for the same source.
func FileID(object ObjectIdentity) string {
	sum := sha256.Sum256([]byte(object.Bucket + "/" + object.Key + "/" + object.VersionID + "/" + object.ETag + "/" + strconv.FormatInt(object.Size, 10)))
	return hex.EncodeToString(sum[:])
}

// OutputMode controls how the Worker groups record envelopes into SQS messages.
//
//   - OutputModeSingle (default): one SQS message per envelope; limited by MaxEventBytes.
//   - OutputModeBundle: multiple envelopes packed into one SQS message; limited by
//     MaxEnvelopesPerMessage and MaxMessageBytes. Consumers must explicitly opt in to
//     the bundle contract and handle BundleEnvelope instead of a plain Envelope.
type OutputMode string

const (
	OutputModeSingle OutputMode = "single" // default; backwards-compatible
	OutputModeBundle OutputMode = "bundle"
)

// PrefixConfiguration is the JSON document stored in SSM for an S3 bucket/key
// prefix. It combines the file contract with the tunable limits that used to
// be defined only at Lambda startup.
type PrefixConfiguration struct {
	Bucket          string   `json:"bucket"`
	Prefix          string   `json:"prefix"`
	DataType        DataType `json:"dataType"`
	RecordsPerChunk int      `json:"recordsPerChunk"`
	BatchSize       int      `json:"batchSize"`
	MaxEventBytes   int      `json:"maxEventBytes"`
	MaxFileBytes    int64    `json:"maxFileBytes"`
	MaxChunkBytes   int64    `json:"maxChunkBytes"`
	// TargetChunkBytes is the nominal text chunk size. Zero selects the
	// automatic planner, which targets approximately 100 chunks per file.
	TargetChunkBytes     int64           `json:"targetChunkBytes,omitempty"`
	JSONArraySearchBytes int             `json:"jsonArraySearchBytes"`
	MaxRecordLengthBytes int64           `json:"maxRecordLengthBytes,omitempty"`
	MultiLineLayout      MultiLineLayout `json:"multiLineLayout,omitempty"`
	JSONArrayLayout      JSONArrayLayout `json:"jsonArrayLayout,omitempty"`
	EventSchemaID        string          `json:"eventSchemaId"`
	EventSchemaVersion   string          `json:"eventSchemaVersion"`
	EventFormat          string          `json:"eventFormat"`
	// Responsible is a declared human-readable identifier for the party that
	// last updated this configuration. It is not an authenticated identity;
	// correlate with CloudTrail for proof of authorship.
	Responsible string `json:"responsible,omitempty"`

	// ── Bundle output mode (T15) ──────────────────────────────────────────────

	// OutputMode selects how envelopes are packed into SQS messages.
	// Omitting or setting "single" preserves the existing per-envelope contract.
	// Set "bundle" to opt in to multi-envelope packing (requires consumer upgrade).
	OutputMode OutputMode `json:"outputMode,omitempty"`
	// MaxEnvelopesPerMessage is the maximum number of envelopes in one bundle
	// message. Ignored in single mode. Must be ≥ 1 when bundle mode is active.
	// Default (0) means use the system default of 100 in bundle mode.
	MaxEnvelopesPerMessage int `json:"maxEnvelopesPerMessage,omitempty"`
	// MaxMessageBytes is the maximum total byte size of one bundle message
	// (serialized JSON). Ignored in single mode. Must be ≤ MaxEventBytes when set.
	// A single envelope that exceeds this limit alone is an explicit error (no truncation).
	MaxMessageBytes int `json:"maxMessageBytes,omitempty"`

	// ── Authorisation and routing (T20) ──────────────────────────────────────

	// PrefixID is the canonical identifier for this bucket/prefix pair. It
	// must be unique across all registered prefixes and is used as the stable
	// key for per-prefix resources (queues, roles, capacity). When empty the
	// prefix is unregistered and only reachable via explicit OrganizerRequest.
	PrefixID string `json:"prefixId,omitempty"`
	// OutputQueueURL overrides the default output queue for events produced by
	// Workers that process files from this prefix. When empty the system-wide
	// OutputQueueURL from Lambda environment is used instead.
	OutputQueueURL string `json:"outputQueueURL,omitempty"`
	// ChunkQueueURL routes this prefix to its exclusive chunk queue. It is
	// mandatory for registered prefixes so no Worker can steal another prefix's
	// work from a shared queue.
	ChunkQueueURL string `json:"chunkQueueURL,omitempty"`
	// AllowedSourceARNs is the set of IAM principal ARNs (role, user, or
	// service) that are permitted to submit OrganizerRequests for files under
	// this prefix. An empty slice means the prefix is open (legacy behaviour).
	// These ARNs are not enforced at the code level; they are stored here so
	// that Terraform can generate corresponding SQS/Lambda resource policies.
	AllowedSourceARNs []string `json:"allowedSourceARNs,omitempty"`

	// MaxActiveJobs is the maximum number of jobs that may be in a non-terminal
	// state for this prefix at any given time. 0 means no quota (unlimited).
	// When the quota is reached, further admissions return ErrQuotaExceeded and
	// the message is allowed to be retried by the SQS visibility timeout without
	// causing a DLQ redrive (the organizer does not fail the batch item).
	MaxActiveJobs int `json:"maxActiveJobs,omitempty"`
}

// ConfigurationSnapshot captures the immutable provenance of a prefix
// configuration as loaded from SSM at admission time. Once attached to a job,
// subsequent SSM changes do not affect that job.
type ConfigurationSnapshot struct {
	// Configuration is the exact prefix configuration used at admission. It is
	// persisted with the job/chunks so replay never needs SSM history to
	// reconstruct the behavior selected for this execution.
	Configuration PrefixConfiguration `json:"configuration"`
	// ConfigHash is the SHA-256 hex digest of the canonical JSON content.
	ConfigHash string `json:"configHash"`
	// ParameterName is the SSM parameter name (or ARN) that supplied the configuration.
	ParameterName string `json:"parameterName"`
	// ParameterVersion is the SSM parameter version returned by GetParameter/GetParametersByPath.
	ParameterVersion int64 `json:"parameterVersion"`
	// Responsible is the declared responsible field from the configuration document.
	Responsible string `json:"responsible,omitempty"`
	// LoadedAt is the instant the configuration was read from SSM (UTC RFC3339Nano).
	LoadedAt string `json:"loadedAt"`
	// GlobalLimitsParameter is the SSM parameter name that supplied the global limits.
	GlobalLimitsParameter string `json:"globalLimitsParameter"`
	// GlobalLimitsVersion is the SSM parameter version of the global limits.
	GlobalLimitsVersion int64 `json:"globalLimitsVersion"`
	// GlobalLimits is the exact limits document used when validating this
	// configuration; it must not be reloaded for an admitted job.
	GlobalLimits GlobalLimits `json:"globalLimits"`
}

type InputTypeLimits struct {
	MaxFileBytes   int64 `json:"maxFileBytes"`
	MaxRecordBytes int64 `json:"maxRecordBytes"`
}

// GlobalLimits defines the ceilings that no per-prefix configuration may
// exceed. The Organizer loads this document from SSM during cold start.
type GlobalLimits struct {
	MaxFileBytes            int64                        `json:"maxFileBytes"`
	MaxChunkBytes           int64                        `json:"maxChunkBytes"`
	MaxEventBytes           int                          `json:"maxEventBytes"`
	MaxBatchSize            int                          `json:"maxBatchSize"`
	MaxJSONArraySearchBytes int                          `json:"maxJsonArraySearchBytes"`
	InputTypes              map[DataType]InputTypeLimits `json:"inputTypes"`
}

// JobConfiguration carries the selected prefix configuration to the Worker.
type JobConfiguration struct {
	BatchSize          int    `json:"batchSize"`
	MaxEventBytes      int    `json:"maxEventBytes"`
	MaxChunkBytes      int64  `json:"maxChunkBytes"`
	TargetChunkBytes   int64  `json:"targetChunkBytes,omitempty"`
	EventSchemaID      string `json:"eventSchemaId"`
	EventSchemaVersion string `json:"eventSchemaVersion"`
	EventFormat        string `json:"eventFormat"`

	// ── Bundle output mode (T15) ──────────────────────────────────────────────

	// OutputMode controls envelope packing. Empty or "single" preserves the
	// existing per-envelope contract; "bundle" opts into BundleEnvelope packing.
	OutputMode OutputMode `json:"outputMode,omitempty"`
	// MaxEnvelopesPerMessage is the maximum number of envelopes per bundle
	// message. 0 means use the system default (100).
	MaxEnvelopesPerMessage int `json:"maxEnvelopesPerMessage,omitempty"`
	// MaxMessageBytes is the ceiling for a serialised bundle message in bytes.
	// 0 means use MaxEventBytes. A single envelope that exceeds this limit
	// produces ErrEnvelopeTooLarge (never truncated).
	MaxMessageBytes int `json:"maxMessageBytes,omitempty"`

	// ── Authorisation and routing (T20) ──────────────────────────────────────

	// OutputQueueURL overrides the Lambda environment's default OutputQueueURL
	// for events produced by this job. When empty the default is used.
	// This is set by the Organizer from PrefixConfiguration.OutputQueueURL so
	// that the Worker never needs to know which prefix it serves.
	OutputQueueURL string `json:"outputQueueURL,omitempty"`
	// PrefixID is the canonical identifier of the registered prefix that owns
	// this job. Empty for legacy/unregistered prefixes.
	PrefixID      string `json:"prefixId,omitempty"`
	ChunkQueueURL string `json:"chunkQueueURL,omitempty"`
}

// OrganizerRequest is the explicit input contract accepted by the organizer.
type OrganizerRequest struct {
	SchemaVersion string `json:"schemaVersion"`
	// ExecutionID identifies one intake occurrence. Lambda fills it from the
	// SQS message ID so retries reuse job/event identities while replay creates new ones.
	ExecutionID string        `json:"executionId,omitempty"`
	Files       []FileRequest `json:"files"`
}

type ChunkJob struct {
	SchemaVersion        string                `json:"schemaVersion"`
	JobID                string                `json:"jobId"`
	FileID               string                `json:"fileId"`
	ChunkID              string                `json:"chunkId"`
	Bucket               string                `json:"bucket"`
	Key                  string                `json:"key"`
	PresignedURL         string                `json:"presignedUrl,omitempty"`
	ETag                 string                `json:"etag"`
	StartRecord          int64                 `json:"startRecord"`
	StartByte            int64                 `json:"startByte"`
	EndByteInclusive     int64                 `json:"endByteInclusive"`
	MaxRecordLengthBytes int64                 `json:"maxRecordLengthBytes,omitempty"`
	TrailingPaddingBytes int64                 `json:"trailingPaddingBytes,omitempty"`
	DataType             DataType              `json:"dataType"`
	MultiLineLayout      MultiLineLayout       `json:"multiLineLayout,omitempty"`
	JSONArrayLayout      JSONArrayLayout       `json:"jsonArrayLayout,omitempty"`
	JSONArrayOffset      int64                 `json:"jsonArrayOffset,omitempty"`
	Context              CorporateContext      `json:"context,omitempty"`
	VersionID            string                `json:"versionId,omitempty"`
	FileSize             int64                 `json:"fileSize,omitempty"`
	Configuration        JobConfiguration      `json:"configuration,omitempty"`
	ConfigSnapshot       ConfigurationSnapshot `json:"configSnapshot,omitempty"`
}

// RecordPayload carries the raw parsed content of a single record.
type RecordPayload struct {
	Raw string `json:"raw,omitempty"`
}

// Envelope is the standard File-to-Envelope contract for every published event.
type Envelope[T any] struct {
	Metadata   Metadata   `json:"metadata"`
	Source     Source     `json:"source"`
	Processing Processing `json:"processing"`
	Data       T          `json:"data"`
}

type Metadata struct {
	EventID        string `json:"eventId"`
	SourceRecordID string `json:"sourceRecordId"`
	Schema         Schema `json:"schema"`
	Format         string `json:"format"`
	CreatedAt      string `json:"createdAt"`
	TransactionID  string `json:"transactionId,omitempty"`
	CorrelationID  string `json:"correlationId,omitempty"`
	TraceID        string `json:"traceId,omitempty"`
}

type Schema struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type Source struct {
	Type       string `json:"type"`
	System     string `json:"system,omitempty"`
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	VersionID  string `json:"versionId,omitempty"`
	ETag       string `json:"etag,omitempty"`
	FileName   string `json:"fileName,omitempty"`
	FileFormat string `json:"fileFormat,omitempty"`
	FileSize   int64  `json:"fileSize,omitempty"`
}

type Processing struct {
	JobID        string `json:"jobId"`
	ChunkID      string `json:"chunkId"`
	RecordNumber *int64 `json:"recordNumber,omitempty"`
	ByteOffset   *int64 `json:"byteOffset,omitempty"`
	ByteLength   *int64 `json:"byteLength,omitempty"`
}

// ── Bundle envelope contract (T15) ───────────────────────────────────────────

// BundleSchemaVersion is the version sent in BundleEnvelope.SchemaVersion.
const BundleSchemaVersion = "f2e-bundle/1"

// BundleEnvelope is the SQS message body used when OutputMode is "bundle".
// It wraps multiple Envelope[RecordPayload] items produced from the same chunk.
//
// A consumer that subscribes to the bundle contract must:
//  1. Inspect the top-level SchemaVersion to distinguish bundles from single envelopes.
//  2. Process Items idempotently: a bundle retry re-delivers ALL items in the bundle.
//     Consumers should track delivered eventIds (e.g. via DynamoDB) and skip duplicates.
//  3. Never assume Items are ordered across bundles from the same chunk.
//
// A single Envelope that serialises to more bytes than MaxMessageBytes is an
// explicit pipeline error (ErrEnvelopeTooLarge). The pipeline never truncates data.
type BundleEnvelope struct {
	// SchemaVersion identifies this message as a bundle. Value: BundleSchemaVersion.
	SchemaVersion string `json:"schemaVersion"`
	// BundleID is the SHA-256 hex digest of the sorted eventIds of all items,
	// making it stable across retries of the same logical bundle.
	BundleID string `json:"bundleId"`
	// JobID and ChunkID identify the source work unit; all items share the same chunk.
	JobID   string `json:"jobId"`
	ChunkID string `json:"chunkId"`
	// Items contains the packed envelopes in publication order within the chunk.
	Items []Envelope[RecordPayload] `json:"items"`
}

// ErrEnvelopeTooLarge is returned when a single serialised envelope exceeds
// MaxMessageBytes. The pipeline never truncates; the caller must reject the record.
type ErrEnvelopeTooLarge struct {
	EventID     string
	ActualBytes int
	LimitBytes  int
}

func (e ErrEnvelopeTooLarge) Error() string {
	return fmt.Sprintf("envelope %s serialises to %d bytes, exceeds MaxMessageBytes %d", e.EventID, e.ActualBytes, e.LimitBytes)
}

// FileSummary contains processing summary for a single file
type FileSummary struct {
	Bucket               string `json:"bucket"`
	Key                  string `json:"key"`
	SizeBytes            int64  `json:"sizeBytes"`
	ChunksGenerated      int64  `json:"chunksGenerated"`
	ProcessingTimeMillis int64  `json:"processingTimeMillis"`
}

// OrganizerSummary contains the summary of the organizer execution
type OrganizerSummary struct {
	SchemaVersion        string        `json:"schemaVersion"`
	StartTimeMillis      int64         `json:"startTimeMillis"`
	EndTimeMillis        int64         `json:"endTimeMillis"`
	ProcessingTimeMillis int64         `json:"processingTimeMillis"`
	FilesProcessed       int           `json:"filesProcessed"`
	TotalChunksGenerated int64         `json:"totalChunksGenerated"`
	Files                []FileSummary `json:"files"`
}

// ChunkProcessingSummary contains summary for a processed chunk
type ChunkProcessingSummary struct {
	ChunkID              string  `json:"chunkId"`
	RecordsProcessed     int64   `json:"recordsProcessed"`
	ProcessingTimeMillis int64   `json:"processingTimeMillis"`
	TPS                  float64 `json:"tps"`
}

// WorkerSummary contains the summary of the worker execution
type WorkerSummary struct {
	SchemaVersion         string                   `json:"schemaVersion"`
	StartTimeMillis       int64                    `json:"startTimeMillis"`
	EndTimeMillis         int64                    `json:"endTimeMillis"`
	ProcessingTimeMillis  int64                    `json:"processingTimeMillis"`
	ChunksProcessed       int                      `json:"chunksProcessed"`
	TotalRecordsProcessed int64                    `json:"totalRecordsProcessed"`
	TPS                   float64                  `json:"tps"`
	Chunks                []ChunkProcessingSummary `json:"chunks"`
}
