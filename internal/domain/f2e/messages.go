// Package f2e defines the messages exchanged by the F2E pipeline.
package f2e

const SchemaVersion = "1"

type FileReference struct {
	Bucket string
	Key    string
}

type ChunkJob struct {
	SchemaVersion     string `json:"schemaVersion"`
	JobID             string `json:"jobId"`
	FileID            string `json:"fileId"`
	ChunkID           string `json:"chunkId"`
	Bucket            string `json:"bucket"`
	Key               string `json:"key"`
	ETag              string `json:"etag"`
	StartRecord       int64  `json:"startRecord"`
	RecordCount       int64  `json:"recordCount"`
	RecordLengthBytes int64  `json:"recordLengthBytes"`
	StartByte         int64  `json:"startByte"`
	EndByteInclusive  int64  `json:"endByteInclusive"`
}
type OutputEvent struct {
	SchemaVersion string `json:"schemaVersion"`
	EventID       string `json:"eventId"`
	FileID        string `json:"fileId"`
	JobID         string `json:"jobId"`
	ChunkID       string `json:"chunkId"`
	RecordNumber  int64  `json:"recordNumber"`
	ByteOffset    int64  `json:"byteOffset"`
	Payload       struct {
		Raw string `json:"raw"`
	} `json:"payload"`
}
