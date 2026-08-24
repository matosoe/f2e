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
)

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
	Options              ProcessingOptions `json:"options,omitempty"`
}
type OutputEvent struct {
	SchemaVersion string   `json:"schemaVersion"`
	EventID       string   `json:"eventId"`
	FileID        string   `json:"fileId"`
	JobID         string   `json:"jobId"`
	ChunkID       string   `json:"chunkId"`
	RecordNumber  int64    `json:"recordNumber"`
	ByteOffset    int64    `json:"byteOffset"`
	DataType      DataType `json:"dataType"`
	Payload       struct {
		Raw    string   `json:"raw,omitempty"`
		Fields []string `json:"fields,omitempty"`
		Base64 string   `json:"base64,omitempty"`
	} `json:"payload"`
}
