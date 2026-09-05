package f2e

import (
	"encoding/json"
	"strings"
	"testing"
)

// ── T15: OutputMode and BundleEnvelope tests ──────────────────────────────────

func TestOutputModeDefaultIsEmpty(t *testing.T) {
	cfg := PrefixConfiguration{Bucket: "b", Prefix: "p/", DataType: DataTypeText}
	if cfg.OutputMode != "" {
		t.Fatalf("expected empty OutputMode, got %q", cfg.OutputMode)
	}
}

func TestOutputModeJSONRoundTrip(t *testing.T) {
	cfg := PrefixConfiguration{
		Bucket: "b", Prefix: "p/", DataType: DataTypeText,
		OutputMode:             OutputModeBundle,
		MaxEnvelopesPerMessage: 50,
		MaxMessageBytes:        200000,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got PrefixConfiguration
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.OutputMode != OutputModeBundle {
		t.Fatalf("OutputMode: want %q, got %q", OutputModeBundle, got.OutputMode)
	}
	if got.MaxEnvelopesPerMessage != 50 {
		t.Fatalf("MaxEnvelopesPerMessage: want 50, got %d", got.MaxEnvelopesPerMessage)
	}
	if got.MaxMessageBytes != 200000 {
		t.Fatalf("MaxMessageBytes: want 200000, got %d", got.MaxMessageBytes)
	}
}

func TestOutputModeSingleOmittedWhenEmpty(t *testing.T) {
	// outputMode should be omitted from JSON when zero-value (backwards-compat).
	cfg := PrefixConfiguration{Bucket: "b", Prefix: "p/", DataType: DataTypeText}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "outputMode") {
		t.Fatalf("outputMode should be absent from JSON for zero-value, got: %s", raw)
	}
}

func TestBundleEnvelopeSchemaVersion(t *testing.T) {
	bundle := BundleEnvelope{
		SchemaVersion: BundleSchemaVersion,
		BundleID:      strings.Repeat("a", 64),
		JobID:         "job1",
		ChunkID:       "chunk1",
		Items:         []Envelope[RecordPayload]{},
	}
	if bundle.SchemaVersion != "f2e-bundle/1" {
		t.Fatalf("expected f2e-bundle/1, got %q", bundle.SchemaVersion)
	}
}

func TestBundleEnvelopeJSONRoundTrip(t *testing.T) {
	rn := int64(1)
	item := Envelope[RecordPayload]{
		Metadata:   Metadata{EventID: strings.Repeat("a", 64), SourceRecordID: strings.Repeat("b", 64), Schema: Schema{ID: "s", Version: "1"}, Format: "json", CreatedAt: "2026-09-05T00:00:00Z"},
		Source:     Source{Type: "s3", Bucket: "bucket", Key: "key", ETag: `"etag"`, FileFormat: "text", FileSize: 100},
		Processing: Processing{JobID: "job1", ChunkID: "chunk1", RecordNumber: &rn},
		Data:       RecordPayload{Raw: "hello"},
	}
	bundle := BundleEnvelope{
		SchemaVersion: BundleSchemaVersion,
		BundleID:      strings.Repeat("c", 64),
		JobID:         "job1",
		ChunkID:       "chunk1",
		Items:         []Envelope[RecordPayload]{item},
	}
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got BundleEnvelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SchemaVersion != BundleSchemaVersion {
		t.Fatalf("schemaVersion: want %q, got %q", BundleSchemaVersion, got.SchemaVersion)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items: want 1, got %d", len(got.Items))
	}
	if got.Items[0].Data.Raw != "hello" {
		t.Fatalf("item data: want hello, got %q", got.Items[0].Data.Raw)
	}
}

func TestErrEnvelopeTooLargeMessage(t *testing.T) {
	err := ErrEnvelopeTooLarge{EventID: strings.Repeat("a", 64), ActualBytes: 300000, LimitBytes: 262144}
	msg := err.Error()
	if !strings.Contains(msg, "300000") || !strings.Contains(msg, "262144") {
		t.Fatalf("error message missing sizes: %q", msg)
	}
}

func TestJobConfigurationBundleFieldsRoundTrip(t *testing.T) {
	cfg := JobConfiguration{
		BatchSize: 10, MaxEventBytes: 256 * 1024, MaxChunkBytes: 64 * 1024 * 1024,
		EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json",
		OutputMode: OutputModeBundle, MaxEnvelopesPerMessage: 100, MaxMessageBytes: 240000,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got JobConfiguration
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.OutputMode != OutputModeBundle || got.MaxEnvelopesPerMessage != 100 || got.MaxMessageBytes != 240000 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestEnvelopeV2AddsIdentityWithoutBreakingUnknownFieldConsumers(t *testing.T) {
	record := int64(1)
	envelope := Envelope[RecordPayload]{
		Metadata:   Metadata{EventID: strings.Repeat("a", 64), SourceRecordID: strings.Repeat("b", 64), Schema: Schema{ID: "record", Version: "2"}, Format: "json", CreatedAt: "2026-09-01T00:00:00Z"},
		Source:     Source{Type: "s3", Bucket: "bucket", Key: "key", ETag: `"etag"`, FileFormat: "text", FileSize: 4},
		Processing: Processing{JobID: "job", ChunkID: "chunk", RecordNumber: &record},
		Data:       RecordPayload{Raw: "abc"},
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	// A v1-style consumer decoding only its known fields continues to work.
	var legacy struct {
		Metadata struct {
			EventID string `json:"eventId"`
		} `json:"metadata"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &legacy); err != nil || legacy.Metadata.EventID != envelope.Metadata.EventID || len(legacy.Data) == 0 {
		t.Fatalf("legacy decode failed: err=%v value=%+v", err, legacy)
	}
	var current Envelope[RecordPayload]
	if err := json.Unmarshal(body, &current); err != nil || current.Metadata.SourceRecordID != envelope.Metadata.SourceRecordID {
		t.Fatalf("v2 round trip failed: err=%v value=%+v", err, current)
	}
}
