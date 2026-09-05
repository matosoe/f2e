// Package f2e defines the messages exchanged by the F2E pipeline.
package f2e

const SchemaVersion = "1"

type FileReference struct {
	Bucket string
	Key    string
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
	PresignedURL         string           `json:"presignedUrl,omitempty"`
	DataType             DataType         `json:"dataType"`
	MaxRecordLengthBytes int64            `json:"maxRecordLengthBytes,omitempty"`
	MultiLineLayout      MultiLineLayout  `json:"multiLineLayout,omitempty"`
	JSONArrayLayout      JSONArrayLayout  `json:"jsonArrayLayout,omitempty"`
	Context              CorporateContext `json:"context,omitempty"`
}

// PrefixConfiguration is the JSON document stored in SSM for an S3 bucket/key
// prefix. It combines the file contract with the tunable limits that used to
// be defined only at Lambda startup.
type PrefixConfiguration struct {
	Bucket               string          `json:"bucket"`
	Prefix               string          `json:"prefix"`
	DataType             DataType        `json:"dataType"`
	RecordsPerChunk      int             `json:"recordsPerChunk"`
	BatchSize            int             `json:"batchSize"`
	MaxEventBytes        int             `json:"maxEventBytes"`
	MaxFileBytes         int64           `json:"maxFileBytes"`
	MaxChunkBytes        int64           `json:"maxChunkBytes"`
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
}

// ConfigurationSnapshot captures the immutable provenance of a prefix
// configuration as loaded from SSM at admission time. Once attached to a job,
// subsequent SSM changes do not affect that job.
type ConfigurationSnapshot struct {
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
	EventSchemaID      string `json:"eventSchemaId"`
	EventSchemaVersion string `json:"eventSchemaVersion"`
	EventFormat        string `json:"eventFormat"`
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
