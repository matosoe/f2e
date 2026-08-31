// Package f2e defines the messages exchanged by the F2E pipeline.
package f2e

const SchemaVersion = "1"

type FileReference struct {
	Bucket string
	Key    string
}

// DataType describes how the object is split and represented by workers.
type DataType string

const (
	DataTypeFixedWidth DataType = "fixed-width"
	DataTypeJSONL      DataType = "jsonl"
	DataTypeNDJSON     DataType = "ndjson"
	DataTypeCSV        DataType = "csv"
	DataTypeBinary     DataType = "binary"
	DataTypeText       DataType = "text"
	DataTypeMultiLine  DataType = "multi-line"
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

// ProcessingOptions are deliberately carried in both organizer and worker
// contracts so each worker has all decisions needed to process its chunk.
type ProcessingOptions struct {
	BypassJSONValidation bool `json:"bypassJsonValidation,omitempty"`
}

type FileRequest struct {
	Bucket               string            `json:"bucket"`
	Key                  string            `json:"key"`
	DataType             DataType          `json:"dataType"`
	MaxRecordLengthBytes int64             `json:"maxRecordLengthBytes,omitempty"`
	MultiLineLayout      MultiLineLayout   `json:"multiLineLayout,omitempty"`
	Options              ProcessingOptions `json:"options,omitempty"`
}

// OrganizerRequest is the explicit input contract accepted by the organizer.
type OrganizerRequest struct {
	SchemaVersion string        `json:"schemaVersion"`
	Files         []FileRequest `json:"files"`
}

type ChunkJob struct {
	SchemaVersion        string            `json:"schemaVersion"`
	JobID                string            `json:"jobId"`
	FileID               string            `json:"fileId"`
	ChunkID              string            `json:"chunkId"`
	Bucket               string            `json:"bucket"`
	Key                  string            `json:"key"`
	ETag                 string            `json:"etag"`
	StartRecord          int64             `json:"startRecord"`
	RecordCount          int64             `json:"recordCount"`
	RecordLengthBytes    int64             `json:"recordLengthBytes"`
	StartByte            int64             `json:"startByte"`
	EndByteInclusive     int64             `json:"endByteInclusive"`
	MaxRecordLengthBytes int64             `json:"maxRecordLengthBytes,omitempty"`
	TrailingPaddingBytes int64             `json:"trailingPaddingBytes,omitempty"`
	DataType             DataType          `json:"dataType"`
	MultiLineLayout      MultiLineLayout   `json:"multiLineLayout,omitempty"`
	Options              ProcessingOptions `json:"options,omitempty"`
	VersionID            string            `json:"versionId,omitempty"`
	FileSize             int64             `json:"fileSize,omitempty"`
}

// RecordPayload carries the raw parsed content of a single record.
type RecordPayload struct {
	Raw    string   `json:"raw,omitempty"`
	Fields []string `json:"fields,omitempty"`
	Base64 string   `json:"base64,omitempty"`
}

// Envelope is the standard File-to-Envelope contract for every published event.
type Envelope[T any] struct {
	Metadata   Metadata   `json:"metadata"`
	Source     Source     `json:"source"`
	Processing Processing `json:"processing"`
	Data       T          `json:"data"`
}

type Metadata struct {
	EventID       string `json:"eventId"`
	Schema        Schema `json:"schema"`
	Format        string `json:"format"`
	CreatedAt     string `json:"createdAt"`
	TransactionID string `json:"transactionId,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
	TraceID       string `json:"traceId,omitempty"`
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
