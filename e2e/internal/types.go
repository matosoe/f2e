package internal

// OrganizerRequest is the explicit input contract sent to the file-intake queue.
type OrganizerRequest struct {
	SchemaVersion string        `json:"schemaVersion"`
	Files         []FileRequest `json:"files"`
}

// FileRequest describes one file to be processed.
type FileRequest struct {
	Bucket               string          `json:"bucket"`
	Key                  string          `json:"key"`
	DataType             string          `json:"dataType"`
	MaxRecordLengthBytes int64           `json:"maxRecordLengthBytes,omitempty"`
	MultiLineLayout      MultiLineLayout `json:"multiLineLayout,omitempty"`
	JSONArrayLayout      JSONArrayLayout `json:"jsonArrayLayout,omitempty"`
}

// MultiLineLayout describes grouping of physical lines into logical records.
type MultiLineLayout struct {
	BreakPosition     int      `json:"breakPosition,omitempty"`
	BreakMarker       string   `json:"breakMarker"`
	AcceptedPrefixes  []string `json:"acceptedPrefixes,omitempty"`
	LineSeparator     string   `json:"lineSeparator,omitempty"`
	MaxBytesPerRecord int64    `json:"maxBytesPerRecord"`
}

// JSONArrayLayout describes how to iterate a JSON array inside a file.
type JSONArrayLayout struct {
	ArrayPath          string `json:"arrayPath,omitempty"`
	MaxBytesPerElement int64  `json:"maxBytesPerElement"`
}

// Envelope is the standard event contract published to the output queue.
type Envelope struct {
	Metadata   EnvMetadata   `json:"metadata"`
	Source     EnvSource     `json:"source"`
	Processing EnvProcessing `json:"processing"`
	Data       EnvData       `json:"data"`
}

// EnvMetadata holds event identification fields.
type EnvMetadata struct {
	EventID   string    `json:"eventId"`
	Schema    EnvSchema `json:"schema"`
	Format    string    `json:"format"`
	CreatedAt string    `json:"createdAt"`
}

// EnvSchema identifies the schema used by the event.
type EnvSchema struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// EnvSource describes the origin of the record.
type EnvSource struct {
	Type       string `json:"type"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	VersionID  string `json:"versionId,omitempty"`
	ETag       string `json:"etag,omitempty"`
	FileName   string `json:"fileName,omitempty"`
	FileFormat string `json:"fileFormat,omitempty"`
	FileSize   int64  `json:"fileSize,omitempty"`
}

// EnvProcessing carries pipeline tracking fields.
type EnvProcessing struct {
	JobID        string `json:"jobId"`
	ChunkID      string `json:"chunkId"`
	RecordNumber *int64 `json:"recordNumber,omitempty"`
	ByteOffset   *int64 `json:"byteOffset,omitempty"`
	ByteLength   *int64 `json:"byteLength,omitempty"`
}

// EnvData holds the raw record content.
type EnvData struct {
	Raw    string   `json:"raw,omitempty"`
	Fields []string `json:"fields,omitempty"`
	Base64 string   `json:"base64,omitempty"`
}
